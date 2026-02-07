require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const webpush = require('web-push');

const app = express();
const PORT = process.env.PORT || 10000;

// ミドルウェア
app.use(cors());
app.use(express.json());

// 環境変数のデバッグ出力
console.log('=== Environment Variables Check ===');
console.log('DATABASE_URL:', process.env.DATABASE_URL ? 'SET (length: ' + process.env.DATABASE_URL.length + ')' : 'NOT SET');
console.log('JWT_SECRET:', process.env.JWT_SECRET ? 'SET' : 'NOT SET');
console.log('VAPID_PUBLIC_KEY:', process.env.VAPID_PUBLIC_KEY ? 'SET' : 'NOT SET');
console.log('VAPID_PRIVATE_KEY:', process.env.VAPID_PRIVATE_KEY ? 'SET' : 'NOT SET');
console.log('===================================');

// データベース接続設定
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// データベース接続テスト
pool.connect((err, client, release) => {
  if (err) {
    console.error('❌ Database connection error:', err.message);
  } else {
    console.log('✅ Database connected successfully');
    release();
  }
});

// VAPID設定
if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(
    'mailto:admin@example.com',
    process.env.VAPID_PUBLIC_KEY,
    process.env.VAPID_PRIVATE_KEY
  );
  console.log('✅ VAPID keys configured');
} else {
  console.warn('⚠️ VAPID keys not set');
}

// 認証ミドルウェア
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }

  jwt.verify(token, process.env.JWT_SECRET || 'default-secret', (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid token' });
    }
    req.user = user;
    next();
  });
};

// サイトアクセス権限チェックミドルウェア
function checkSiteAccess(req, res, next) {
  const userRole = req.user.role;
  const userSiteId = req.user.assigned_site_id;
  
  // adminは全サイトアクセス可能
  if (userRole === 'admin') {
    return next();
  }
  
  // clientは自分のサイトのみ
  const requestedSiteId = req.query.siteId || req.body.siteId || req.params.siteId;
  
  if (requestedSiteId && requestedSiteId !== userSiteId) {
    return res.status(403).json({ error: 'このサイトへのアクセス権限がありません' });
  }
  
  // siteIdが指定されていない場合は自動的に設定
  if (!req.query.siteId && !req.body.siteId) {
    req.query.siteId = userSiteId;
    req.body.siteId = userSiteId;
  }
  
  next();
}

// ヘルスチェックエンドポイント
app.get('/health', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW()');
    res.json({
      status: 'ok',
      database: 'connected',
      time: result.rows[0].now,
      environment: {
        databaseUrl: !!process.env.DATABASE_URL,
        jwtSecret: !!process.env.JWT_SECRET,
        vapidKeys: !!(process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY)
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

// ルートエンドポイント
app.get('/', (req, res) => {
  res.json({
    message: 'Web Push Notification API',
    version: '1.0.0',
    endpoints: {
      health: '/health',
      auth: {
        login: 'POST /api/auth/login',
        register: 'POST /api/auth/register'
      },
      subscribers: {
        subscribe: 'POST /api/subscribe',
        list: 'GET /api/subscribers'
      },
      campaigns: {
        create: 'POST /api/campaigns',
        list: 'GET /api/campaigns',
        send: 'POST /api/campaigns/:id/send'
      }
    }
  });
});

// 認証エンドポイント
app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, password, name } = req.body;
    
    const hashedPassword = await bcrypt.hash(password, 10);
    
    const result = await pool.query(
      'INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id, email, name',
      [email, hashedPassword, name]
    );
    
    res.status(201).json({
      message: 'User created successfully',
      user: result.rows[0]
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    
    const result = await pool.query(
      'SELECT * FROM users WHERE email = $1',
      [email]
    );
    
    if (result.rows.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    const user = result.rows[0];
    const validPassword = await bcrypt.compare(password, user.password_hash);
    
    if (!validPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    // JWTトークンにroleとassigned_site_idを含める
    const token = jwt.sign(
      { 
        id: user.id, 
        email: user.email,
        role: user.role || 'client',
        assigned_site_id: user.assigned_site_id
      },
      process.env.JWT_SECRET || 'default-secret',
      { expiresIn: '24h' }
    );
    
    res.json({
      token,
      user: {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role || 'client',
        assigned_site_id: user.assigned_site_id
      }
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 購読エンドポイント
app.post('/api/subscribe', async (req, res) => {
  try {
    const { siteId, subscription, userAgent } = req.body;
    
    // User-Agentを解析
    const deviceInfo = parseUserAgent(userAgent || '');
    
    const result = await pool.query(
      `INSERT INTO subscribers (site_id, endpoint, p256dh_key, auth_key, user_agent, device_type, browser, os)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (site_id, endpoint) DO UPDATE
       SET last_active_at = CURRENT_TIMESTAMP,
           device_type = EXCLUDED.device_type,
           browser = EXCLUDED.browser,
           os = EXCLUDED.os
       RETURNING id`,
      [
        siteId,
        subscription.endpoint,
        subscription.keys.p256dh,
        subscription.keys.auth,
        userAgent,
        deviceInfo.device,
        deviceInfo.browser,
        deviceInfo.os
      ]
    );
    
    res.json({
      message: 'Subscription saved',
      subscriberId: result.rows[0].id
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// User-Agent解析関数（購読エンドポイントの前に追加）
function parseUserAgent(userAgent) {
  const ua = userAgent.toLowerCase();
  
  // デバイス判定
  let device = 'Desktop';
  if (/(tablet|ipad|playbook|silk)|(android(?!.*mobi))/i.test(userAgent)) {
    device = 'Tablet';
  } else if (/Mobile|iP(hone|od)|Android|BlackBerry|IEMobile|Kindle|Silk-Accelerated|(hpw|web)OS|Opera M(obi|ini)/.test(userAgent)) {
    device = 'Mobile';
  }
  
  // ブラウザ判定
  let browser = 'Unknown';
  if (ua.includes('edg/')) {
    browser = 'Edge';
  } else if (ua.includes('chrome') && !ua.includes('edg')) {
    browser = 'Chrome';
  } else if (ua.includes('safari') && !ua.includes('chrome')) {
    browser = 'Safari';
  } else if (ua.includes('firefox')) {
    browser = 'Firefox';
  } else if (ua.includes('opera') || ua.includes('opr/')) {
    browser = 'Opera';
  }
  
  // OS判定
  let os = 'Unknown';
  if (ua.includes('windows')) {
    os = 'Windows';
  } else if (ua.includes('mac os')) {
    os = 'macOS';
  } else if (ua.includes('iphone') || ua.includes('ipad')) {
    os = 'iOS';
  } else if (ua.includes('android')) {
    os = 'Android';
  } else if (ua.includes('linux')) {
    os = 'Linux';
  }
  
  return { device, browser, os };
}

// 購読者一覧
app.get('/api/subscribers', authenticateToken, async (req, res) => {
  try {
    let { siteId } = req.query;
    
    // clientユーザーは自分のサイトIDを強制
    if (req.user.role === 'client') {
      if (!req.user.assigned_site_id) {
        return res.status(403).json({ error: 'サイトが割り当てられていません' });
      }
      siteId = req.user.assigned_site_id;
    }
    
    if (!siteId) {
      return res.status(400).json({ error: 'siteId is required' });
    }
    
    const result = await pool.query(
      'SELECT * FROM subscribers WHERE site_id = $1 AND is_active = true ORDER BY subscribed_at DESC',
      [siteId]
    );
    
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// キャンペーン作成
app.post('/api/campaigns', authenticateToken, async (req, res) => {
  try {
    let { siteId, name, title, body, url, iconUrl, deliveryType, scheduledAt, recurringSchedule } = req.body;
    
    // clientユーザーは自分のサイトIDを強制
    if (req.user.role === 'client') {
      if (!req.user.assigned_site_id) {
        return res.status(403).json({ error: 'サイトが割り当てられていません' });
      }
      siteId = req.user.assigned_site_id;
    }
    
    if (!siteId) {
      return res.status(400).json({ error: 'siteId is required' });
    }
    
    // 繰り返し配信の場合、初回scheduled_atを計算
    if (deliveryType === 'recurring' && recurringSchedule) {
      scheduledAt = calculateNextScheduledTime(recurringSchedule);
    }
    
    const result = await pool.query(
      `INSERT INTO campaigns (site_id, name, title, body, url, icon_url, delivery_type, scheduled_at, recurring_schedule, created_by, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING *`,
      [
        siteId, 
        name, 
        title, 
        body, 
        url, 
        iconUrl || null, 
        deliveryType, 
        scheduledAt, 
        recurringSchedule ? JSON.stringify(recurringSchedule) : null,
        req.user.id, 
        'draft'
      ]
    );
    
    res.status(201).json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// キャンペーン一覧
app.get('/api/campaigns', authenticateToken, async (req, res) => {
  try {
    let { siteId } = req.query;
    
    // clientユーザーは自分のサイトIDを強制
    if (req.user.role === 'client') {
      if (!req.user.assigned_site_id) {
        return res.status(403).json({ error: 'サイトが割り当てられていません' });
      }
      siteId = req.user.assigned_site_id;
    }
    
    if (!siteId) {
      return res.status(400).json({ error: 'siteId is required' });
    }
    
    const result = await pool.query(
      'SELECT * FROM campaigns WHERE site_id = $1 ORDER BY created_at DESC',
      [siteId]
    );
    
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// キャンペーン更新
app.patch('/api/campaigns/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const { scheduled_at, recurring_schedule, status } = req.body;
    
    const updates = [];
    const values = [];
    let paramIndex = 1;
    
    if (scheduled_at !== undefined) {
      updates.push(`scheduled_at = $${paramIndex++}`);
      values.push(scheduled_at);
    }
    
    if (recurring_schedule !== undefined) {
      updates.push(`recurring_schedule = $${paramIndex++}`);
      values.push(recurring_schedule ? JSON.stringify(recurring_schedule) : null);
    }
    
    if (status !== undefined) {
      updates.push(`status = $${paramIndex++}`);
      values.push(status);
    }
    
    if (updates.length === 0) {
      return res.status(400).json({ error: 'No updates provided' });
    }
    
    updates.push(`updated_at = CURRENT_TIMESTAMP`);
    values.push(id);
    
    const query = `UPDATE campaigns SET ${updates.join(', ')} WHERE id = $${paramIndex} RETURNING *`;
    
    const result = await pool.query(query, values);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// キャンペーン削除
app.delete('/api/campaigns/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await pool.query(
      'DELETE FROM campaigns WHERE id = $1 RETURNING *',
      [id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    
    res.json({ message: 'Campaign deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============================================
// サイト管理API
// ============================================

// サイト一覧取得
app.get('/api/sites', authenticateToken, async (req, res) => {
  try {
    let query = `SELECT s.*, 
            (SELECT COUNT(*) FROM subscribers WHERE site_id = s.id AND is_active = true) as subscriber_count,
            (SELECT COUNT(*) FROM campaigns WHERE site_id = s.id) as campaign_count
     FROM sites s
     WHERE s.is_active = true`;
    
    let params = [];
    
    // clientユーザーは自分のサイトのみ表示
    if (req.user.role === 'client' && req.user.assigned_site_id) {
      query += ' AND s.id = $1';
      params.push(req.user.assigned_site_id);
    }
    
    query += ' ORDER BY s.created_at DESC';
    
    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// サイト詳細取得
app.get('/api/sites/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    // clientユーザーは自分のサイトのみアクセス可能
    if (req.user.role === 'client' && req.user.assigned_site_id !== id) {
      return res.status(403).json({ error: 'このサイトへのアクセス権限がありません' });
    }
    
    const result = await pool.query(
      `SELECT s.*, 
              (SELECT COUNT(*) FROM subscribers WHERE site_id = s.id AND is_active = true) as subscriber_count,
              (SELECT COUNT(*) FROM campaigns WHERE site_id = s.id) as campaign_count
       FROM sites s
       WHERE s.id = $1`,
      [id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Site not found' });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// サイト作成
app.post('/api/sites', authenticateToken, async (req, res) => {
  try {
    const { clientName, domain, description, widgetPosition, widgetTheme } = req.body;
    const userId = req.user.id;
    
    console.log('受信したデータ:', { clientName, domain, description, widgetPosition, widgetTheme });
    
    // 必須フィールドのチェック
    if (!clientName || !domain) {
      return res.status(400).json({ error: 'クライアント名とサイトURLは必須です' });
    }
    
    // VAPID鍵のチェック
    if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY) {
      return res.status(500).json({ error: 'VAPID鍵が設定されていません' });
    }
    
    // ドメインの重複チェック
    const existing = await pool.query(
      'SELECT id FROM sites WHERE domain = $1',
      [domain]
    );
    
    if (existing.rows.length > 0) {
      return res.status(400).json({ error: 'このドメインは既に登録されています' });
    }
    
    // domainをベースにURLを生成
    const siteUrl = domain.startsWith('http') ? domain : `https://${domain}`;
    
    // API Keyを生成（ランダムな64文字の16進数）
    const crypto = require('crypto');
    const apiKey = crypto.randomBytes(32).toString('hex');
    
    const result = await pool.query(
      `INSERT INTO sites (
        owner_id,
        name, 
        domain, 
        url, 
        client_name, 
        description, 
        widget_position, 
        widget_theme, 
        vapid_public_key, 
        vapid_private_key,
        api_key,
        settings,
        is_active,
        created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, true, $13)
      RETURNING *`,
      [
        userId,                           // owner_id
        clientName || 'Unnamed Site',     // name
        domain,                           // domain
        siteUrl,                          // url
        clientName,                       // client_name
        description || '',                // description
        widgetPosition || 'bottom-right', // widget_position
        widgetTheme || 'purple',          // widget_theme
        process.env.VAPID_PUBLIC_KEY,     // vapid_public_key
        process.env.VAPID_PRIVATE_KEY,    // vapid_private_key
        apiKey,                           // api_key
        JSON.stringify({}),               // settings（空のJSON）
        userId                            // created_by
      ]
    );
    
    res.json({ 
      message: 'Site created successfully', 
      site: result.rows[0] 
    });
  } catch (error) {
    console.error('サイト作成エラー:', error);
    res.status(500).json({ error: error.message });
  }
});

// サイト更新
app.patch('/api/sites/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const { clientName, domain, description, widgetPosition, widgetTheme, isActive } = req.body;
    
    // ドメインの重複チェック（自分以外）
    if (domain) {
      const existing = await pool.query(
        'SELECT id FROM sites WHERE domain = $1 AND id != $2',
        [domain, id]
      );
      
      if (existing.rows.length > 0) {
        return res.status(400).json({ error: 'このドメインは既に登録されています' });
      }
    }
    
    const result = await pool.query(
      `UPDATE sites 
       SET client_name = COALESCE($1, client_name),
           name = COALESCE($1, name),
           domain = COALESCE($2, domain),
           description = COALESCE($3, description),
           widget_position = COALESCE($4, widget_position),
           widget_theme = COALESCE($5, widget_theme),
           is_active = COALESCE($6, is_active)
       WHERE id = $7
       RETURNING *`,
      [clientName, domain, description, widgetPosition, widgetTheme, isActive, id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Site not found' });
    }
    
    res.json({ 
      message: 'Site updated successfully', 
      site: result.rows[0] 
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// サイト削除（論理削除）
app.delete('/api/sites/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    // デフォルトサイトは削除できない
    const siteCheck = await pool.query(
      'SELECT domain FROM sites WHERE id = $1',
      [id]
    );
    
    if (siteCheck.rows.length === 0) {
      return res.status(404).json({ error: 'Site not found' });
    }
    
    // 論理削除
    await pool.query(
      'UPDATE sites SET is_active = false WHERE id = $1',
      [id]
    );
    
    res.json({ message: 'Site deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 次回実行日時を計算
function calculateNextScheduledTime(recurringSchedule, lastExecuted = null) {
  const { frequency, hour, minute, dayOfWeek, dayOfMonth } = recurringSchedule;
  
  // 基準日時（最後の実行日時 or 現在時刻）
  const baseDate = lastExecuted ? new Date(lastExecuted) : new Date();
  const nextDate = new Date(baseDate);
  
  // 時刻を設定
  nextDate.setHours(parseInt(hour) || 0);
  nextDate.setMinutes(parseInt(minute) || 0);
  nextDate.setSeconds(0);
  nextDate.setMilliseconds(0);
  
  switch (frequency) {
    case 'daily':
      // 翌日の同時刻（既に今日の時刻を過ぎている場合は明日）
      if (nextDate <= baseDate) {
        nextDate.setDate(nextDate.getDate() + 1);
      }
      break;
      
    case 'weekly':
      // 次の指定曜日
      const targetDay = parseInt(dayOfWeek) || 0;
      const currentDay = nextDate.getDay();
      let daysUntilNext = targetDay - currentDay;
      
      // 同じ曜日で時刻が過ぎている場合、または曜日が過去の場合は来週
      if (daysUntilNext < 0 || (daysUntilNext === 0 && nextDate <= baseDate)) {
        daysUntilNext += 7;
      }
      
      nextDate.setDate(nextDate.getDate() + daysUntilNext);
      break;
      
    case 'monthly':
      // 次月の指定日
      const targetDate = parseInt(dayOfMonth) || 1;
      
      // 今月の指定日がまだ来ていない場合は今月、過ぎている場合は来月
      nextDate.setDate(targetDate);
      if (nextDate <= baseDate) {
        nextDate.setMonth(nextDate.getMonth() + 1);
      }
      
      // 月末を超える場合は月末日に調整
      const daysInMonth = new Date(nextDate.getFullYear(), nextDate.getMonth() + 1, 0).getDate();
      nextDate.setDate(Math.min(targetDate, daysInMonth));
      break;
  }
  
  return nextDate.toISOString();
}

// キャンペーン送信処理（共通関数）
async function sendCampaignNotifications(campaign) {
  try {
    // 購読者取得
    const subscribersResult = await pool.query(
      'SELECT * FROM subscribers WHERE site_id = $1 AND is_active = true',
      [campaign.site_id]
    );
    
    const payload = JSON.stringify({
      title: campaign.title,
      body: campaign.body,
      url: campaign.url,
      icon: campaign.icon_url
    });
    
    let successCount = 0;
    let failCount = 0;
    
    // 通知送信
    for (const subscriber of subscribersResult.rows) {
      try {
        const subscription = {
          endpoint: subscriber.endpoint,
          keys: {
            p256dh: subscriber.p256dh_key,
            auth: subscriber.auth_key
          }
        };
        
        await webpush.sendNotification(subscription, payload);
        
        // 配信ログ記録
        await pool.query(
          `INSERT INTO deliveries (campaign_id, subscriber_id, status, sent_at)
           VALUES ($1, $2, $3, CURRENT_TIMESTAMP)`,
          [campaign.id, subscriber.id, 'sent']
        );
        
        successCount++;
      } catch (error) {
        failCount++;
        
        // エラーログ記録
        await pool.query(
          `INSERT INTO deliveries (campaign_id, subscriber_id, status, error_message)
           VALUES ($1, $2, $3, $4)`,
          [campaign.id, subscriber.id, 'failed', error.message]
        );
      }
    }
    
    // キャンペーンステータス更新（日時指定配信のみ）
    if (campaign.delivery_type !== 'recurring') {
      await pool.query(
        'UPDATE campaigns SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2',
        ['sent', campaign.id]
      );
    }
    
    return {
      success: successCount,
      failed: failCount,
      total: subscribersResult.rows.length
    };
  } catch (error) {
    console.error('通知送信エラー:', error);
    throw error;
  }
}

// キャンペーン送信エンドポイント
app.post('/api/campaigns/:id/send', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    // キャンペーン取得
    const campaignResult = await pool.query(
      'SELECT * FROM campaigns WHERE id = $1',
      [id]
    );
    
    if (campaignResult.rows.length === 0) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    
    const campaign = campaignResult.rows[0];
    
    // 通知送信
    const results = await sendCampaignNotifications(campaign);
    
    res.json({
      message: 'Campaign sent',
      results
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// メール設定の保存
app.post('/api/email-settings', authenticateToken, async (req, res) => {
  try {
    const { siteId, settings } = req.body;
    
    const result = await pool.query(
      `INSERT INTO email_settings (site_id, settings, updated_at)
       VALUES ($1, $2, CURRENT_TIMESTAMP)
       ON CONFLICT (site_id) 
       DO UPDATE SET settings = $2, updated_at = CURRENT_TIMESTAMP
       RETURNING *`,
      [siteId, JSON.stringify(settings)]
    );
    
    res.json({ message: 'Settings saved', settings: result.rows[0] });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// メール設定の取得
app.get('/api/email-settings', authenticateToken, async (req, res) => {
  try {
    const { siteId } = req.query;
    
    const result = await pool.query(
      'SELECT * FROM email_settings WHERE site_id = $1',
      [siteId]
    );
    
    if (result.rows.length > 0) {
      res.json({ settings: result.rows[0].settings });
    } else {
      res.json({ settings: null });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// テストメール送信
app.post('/api/email-settings/test', authenticateToken, async (req, res) => {
  try {
    const { siteId, recipients } = req.body;
    
    // ここでは実際のメール送信は行わず、成功レスポンスを返す
    // 実際の実装では、SendGrid、AWS SES、Nodemailerなどを使用
    console.log('Test email would be sent to:', recipients);
    
    res.json({ message: 'Test email sent successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// クライアントユーザー作成（admin専用）
app.post('/api/users/client', authenticateToken, async (req, res) => {
  try {
    // admin権限チェック
    if (req.user.role !== 'admin') {
      return res.status(403).json({ error: '管理者のみ実行可能です' });
    }
    
    const { email, password, name, siteId } = req.body;
    
    // 入力チェック
    if (!email || !password || !siteId) {
      return res.status(400).json({ error: 'メールアドレス、パスワード、サイトIDは必須です' });
    }
    
    // メールアドレスの重複チェック
    const existing = await pool.query('SELECT id FROM users WHERE email = $1', [email]);
    if (existing.rows.length > 0) {
      return res.status(400).json({ error: 'このメールアドレスは既に登録されています' });
    }
    
    // サイトの存在チェック
    const siteCheck = await pool.query('SELECT id FROM sites WHERE id = $1', [siteId]);
    if (siteCheck.rows.length === 0) {
      return res.status(400).json({ error: '指定されたサイトが見つかりません' });
    }
    
    // パスワードハッシュ化
    const hashedPassword = await bcrypt.hash(password, 10);
    
    const result = await pool.query(
      `INSERT INTO users (email, password_hash, name, role, assigned_site_id, created_at)
       VALUES ($1, $2, $3, 'client', $4, CURRENT_TIMESTAMP)
       RETURNING id, email, name, role, assigned_site_id`,
      [email, hashedPassword, name || email, siteId]
    );
    
    res.json({ 
      message: 'クライアントユーザーを作成しました', 
      user: result.rows[0] 
    });
  } catch (error) {
    console.error('クライアントユーザー作成エラー:', error);
    res.status(500).json({ error: error.message });
  }
});

// クライアントユーザー一覧（admin専用）
app.get('/api/users/clients', authenticateToken, async (req, res) => {
  try {
    // admin権限チェック
    if (req.user.role !== 'admin') {
      return res.status(403).json({ error: '管理者のみ実行可能です' });
    }
    
    const result = await pool.query(
      `SELECT u.id, u.email, u.name, u.role, u.assigned_site_id, u.created_at,
              s.name as site_name, s.domain as site_domain
       FROM users u
       LEFT JOIN sites s ON u.assigned_site_id = s.id
       WHERE u.role = 'client'
       ORDER BY u.created_at DESC`
    );
    
    res.json(result.rows);
  } catch (error) {
    console.error('クライアント一覧取得エラー:', error);
    res.status(500).json({ error: error.message });
  }
});

// クライアントユーザー削除（admin専用）
app.delete('/api/users/client/:id', authenticateToken, async (req, res) => {
  try {
    // admin権限チェック
    if (req.user.role !== 'admin') {
      return res.status(403).json({ error: '管理者のみ実行可能です' });
    }
    
    const { id } = req.params;
    
    // 自分自身は削除できない
    if (id === req.user.id) {
      return res.status(400).json({ error: '自分自身は削除できません' });
    }
    
    const result = await pool.query(
      'DELETE FROM users WHERE id = $1 AND role = $2 RETURNING id',
      [id, 'client']
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'ユーザーが見つかりません' });
    }
    
    res.json({ message: 'クライアントユーザーを削除しました' });
  } catch (error) {
    console.error('クライアント削除エラー:', error);
    res.status(500).json({ error: error.message });
  }
});

// 予約キャンペーン一覧取得
app.get('/api/campaigns/scheduled', authenticateToken, checkSiteAccess, async (req, res) => {
  try {
    const { siteId } = req.query;
    
    let query = `
      SELECT c.*, s.client_name, s.domain,
             (SELECT COUNT(*) FROM deliveries WHERE campaign_id = c.id) as delivery_count
      FROM campaigns c
      JOIN sites s ON c.site_id = s.id
      WHERE c.delivery_type IN ('scheduled', 'recurring')
      AND c.status = 'draft'
    `;
    
    const params = [];
    
    if (siteId) {
      query += ' AND c.site_id = $1';
      params.push(siteId);
    }
    
    query += ' ORDER BY c.scheduled_at ASC';
    
    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (error) {
    console.error('予約一覧取得エラー:', error);
    res.status(500).json({ error: error.message });
  }
});

// 予約キャンペーンキャンセル
app.delete('/api/campaigns/scheduled/:id', authenticateToken, checkSiteAccess, async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await pool.query(
      `UPDATE campaigns 
       SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP 
       WHERE id = $1 AND status = 'draft'
       RETURNING *`,
      [id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'キャンペーンが見つかりません' });
    }
    
    res.json({ 
      message: '予約をキャンセルしました',
      campaign: result.rows[0]
    });
  } catch (error) {
    console.error('予約キャンセルエラー:', error);
    res.status(500).json({ error: error.message });
  }
});

// 繰り返し配信停止
app.post('/api/campaigns/recurring/:id/stop', authenticateToken, checkSiteAccess, async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await pool.query(
      `UPDATE campaigns 
       SET status = 'stopped', updated_at = CURRENT_TIMESTAMP 
       WHERE id = $1 AND delivery_type = 'recurring' AND status = 'draft'
       RETURNING *`,
      [id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: '繰り返し配信が見つかりません' });
    }
    
    res.json({ 
      message: '繰り返し配信を停止しました',
      campaign: result.rows[0]
    });
  } catch (error) {
    console.error('繰り返し配信停止エラー:', error);
    res.status(500).json({ error: error.message });
  }
});

// スケジュールされたキャンペーンを実行（1分ごと）
async function executeScheduledCampaigns() {
  try {
    const now = new Date().toISOString();
    
    // 実行すべきキャンペーンを取得（scheduled と recurring 両方）
    const result = await pool.query(
      `SELECT * FROM campaigns 
       WHERE delivery_type IN ('scheduled', 'recurring')
       AND status = 'draft' 
       AND scheduled_at <= $1`,
      [now]
    );
    
    if (result.rows.length > 0) {
      console.log(`📅 ${result.rows.length}件のスケジュール配信を実行中...`);
    }
    
    for (const campaign of result.rows) {
      try {
        console.log(`  → キャンペーン「${campaign.title}」を送信中...`);
        const results = await sendCampaignNotifications(campaign);
        console.log(`  ✅ 送信完了: 成功${results.success}件, 失敗${results.failed}件`);
        
        // 繰り返し配信の場合は次回実行日時を計算
        if (campaign.delivery_type === 'recurring' && campaign.recurring_schedule) {
          const nextTime = calculateNextScheduledTime(campaign.recurring_schedule, campaign.scheduled_at);
          
          await pool.query(
            `UPDATE campaigns 
             SET scheduled_at = $1, updated_at = CURRENT_TIMESTAMP 
             WHERE id = $2`,
            [nextTime, campaign.id]
          );
          
          console.log(`  🔄 次回配信: ${new Date(nextTime).toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' })}`);
        }
        // 日時指定配信の場合はステータスを送信済みに更新（sendCampaignNotifications内で既に更新済み）
        
      } catch (error) {
        console.error(`  ❌ キャンペーン送信エラー (ID: ${campaign.id}):`, error.message);
      }
    }
  } catch (error) {
    console.error('❌ スケジュール実行エラー:', error.message);
  }
}

// スケジューラー起動（1分ごとにチェック）
let schedulerInterval;
function startScheduler() {
  console.log('⏰ スケジューラーを起動しました（1分間隔）');
  
  // 即座に1回実行
  executeScheduledCampaigns();
  
  // 1分ごとに実行
  schedulerInterval = setInterval(executeScheduledCampaigns, 60000);
}

// サーバー起動
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Web Push API server running on port ${PORT}`);
  
  // スケジューラー起動
  startScheduler();
});

// グレースフルシャットダウン
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing HTTP server');
  
  // スケジューラー停止
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
    console.log('⏰ スケジューラーを停止しました');
  }
  
  pool.end(() => {
    console.log('Database pool closed');
    process.exit(0);
  });
});
