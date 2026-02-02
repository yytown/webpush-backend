// Backend API for Web Push Notification Service
// Node.js + Express + PostgreSQL

const express = require('express');
const cors = require('cors');
const webpush = require('web-push');
const { Pool } = require('pg');
const crypto = require('crypto');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const schedulerService = require('./scheduler');
const reportService = require('./report');
const emailReportService = require('./email-report');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// ミドルウェア
app.use(cors());
app.use(express.json());

// PostgreSQL接続
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'webpush',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'password',
});

// VAPID鍵の設定（環境変数から取得）
webpush.setVapidDetails(
  'mailto:your-email@example.com',
  process.env.VAPID_PUBLIC_KEY,
  process.env.VAPID_PRIVATE_KEY
);

// JWTシークレット
const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key';

// 認証ミドルウェア
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid token' });
    }
    req.user = user;
    next();
  });
};

// ==================== 認証API ====================

// ユーザー登録
app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, password, name } = req.body;
    
    // パスワードハッシュ化
    const passwordHash = await bcrypt.hash(password, 10);
    
    const result = await pool.query(
      'INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id, email, name',
      [email, passwordHash, name]
    );
    
    res.json({ user: result.rows[0] });
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: 'Registration failed' });
  }
});

// ログイン
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
    
    // JWT発行
    const token = jwt.sign(
      { userId: user.id, email: user.email },
      JWT_SECRET,
      { expiresIn: '7d' }
    );
    
    // 最終ログイン更新
    await pool.query(
      'UPDATE users SET last_login_at = CURRENT_TIMESTAMP WHERE id = $1',
      [user.id]
    );
    
    res.json({
      token,
      user: {
        id: user.id,
        email: user.email,
        name: user.name
      }
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Login failed' });
  }
});

// ==================== サイト管理API ====================

// サイト一覧取得
app.get('/api/sites', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT s.*, COUNT(sub.id) as subscriber_count
      FROM sites s
      LEFT JOIN subscribers sub ON s.id = sub.site_id AND sub.is_active = true
      INNER JOIN site_permissions sp ON s.id = sp.site_id
      WHERE sp.user_id = $1
      GROUP BY s.id
      ORDER BY s.created_at DESC
    `, [req.user.userId]);
    
    res.json({ sites: result.rows });
  } catch (error) {
    console.error('Get sites error:', error);
    res.status(500).json({ error: 'Failed to get sites' });
  }
});

// サイト作成
app.post('/api/sites', authenticateToken, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    const { name, url, icon_url } = req.body;
    
    // VAPID鍵生成
    const vapidKeys = webpush.generateVAPIDKeys();
    
    // API鍵生成
    const apiKey = crypto.randomBytes(32).toString('hex');
    
    // サイト作成
    const siteResult = await client.query(`
      INSERT INTO sites (owner_id, name, url, icon_url, vapid_public_key, vapid_private_key, api_key)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *
    `, [req.user.userId, name, url, icon_url, vapidKeys.publicKey, vapidKeys.privateKey, apiKey]);
    
    const site = siteResult.rows[0];
    
    // オーナー権限付与
    await client.query(`
      INSERT INTO site_permissions (user_id, site_id, role)
      VALUES ($1, $2, 'owner')
    `, [req.user.userId, site.id]);
    
    await client.query('COMMIT');
    
    res.json({ site });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Create site error:', error);
    res.status(500).json({ error: 'Failed to create site' });
  } finally {
    client.release();
  }
});

// ==================== 購読管理API ====================

// プッシュ通知購読
app.post('/api/subscribe', async (req, res) => {
  try {
    const { siteId, subscription, userAgent, deviceType, browser, os, userIdentifier } = req.body;
    
    const result = await pool.query(`
      INSERT INTO subscribers (
        site_id, endpoint, p256dh_key, auth_key, 
        user_agent, device_type, browser, os, user_identifier
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT (site_id, endpoint) 
      DO UPDATE SET 
        last_active_at = CURRENT_TIMESTAMP,
        is_active = true,
        unsubscribed_at = NULL
      RETURNING id
    `, [
      siteId,
      subscription.endpoint,
      subscription.keys.p256dh,
      subscription.keys.auth,
      userAgent,
      deviceType,
      browser,
      os,
      userIdentifier
    ]);
    
    res.json({ subscriberId: result.rows[0].id, success: true });
  } catch (error) {
    console.error('Subscribe error:', error);
    res.status(500).json({ error: 'Subscription failed' });
  }
});

// プッシュ通知解除
app.post('/api/unsubscribe', async (req, res) => {
  try {
    const { endpoint } = req.body;
    
    await pool.query(`
      UPDATE subscribers 
      SET is_active = false, unsubscribed_at = CURRENT_TIMESTAMP
      WHERE endpoint = $1
    `, [endpoint]);
    
    res.json({ success: true });
  } catch (error) {
    console.error('Unsubscribe error:', error);
    res.status(500).json({ error: 'Unsubscribe failed' });
  }
});

// 購読者一覧
app.get('/api/sites/:siteId/subscribers', authenticateToken, async (req, res) => {
  try {
    const { siteId } = req.params;
    
    const result = await pool.query(`
      SELECT 
        id, device_type, browser, os, 
        subscribed_at, last_active_at, is_active, tags
      FROM subscribers
      WHERE site_id = $1
      ORDER BY subscribed_at DESC
      LIMIT 100
    `, [siteId]);
    
    res.json({ subscribers: result.rows });
  } catch (error) {
    console.error('Get subscribers error:', error);
    res.status(500).json({ error: 'Failed to get subscribers' });
  }
});

// ==================== キャンペーン管理API ====================

// キャンペーン作成
app.post('/api/campaigns', authenticateToken, async (req, res) => {
  try {
    const {
      siteId, name, title, body, url, iconUrl, imageUrl,
      segmentId, deliveryType, scheduledAt, recurringSchedule
    } = req.body;
    
    // スケジュール配信の場合、日時チェック
    if (deliveryType === 'scheduled' && scheduledAt) {
      const scheduledDate = new Date(scheduledAt);
      if (scheduledDate <= new Date()) {
        return res.status(400).json({ error: 'Scheduled time must be in the future' });
      }
    }
    
    const result = await pool.query(`
      INSERT INTO campaigns (
        site_id, name, title, body, url, icon_url, image_url,
        segment_id, delivery_type, scheduled_at, recurring_schedule, 
        created_by, status
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
      RETURNING *
    `, [
      siteId, name, title, body, url, iconUrl, imageUrl,
      segmentId, deliveryType, scheduledAt, recurringSchedule ? JSON.stringify(recurringSchedule) : null,
      req.user.userId,
      deliveryType === 'immediate' ? 'sending' : 
      deliveryType === 'scheduled' ? 'scheduled' : 'active'
    ]);
    
    const campaign = result.rows[0];
    
    // 配信タイプに応じて処理
    if (deliveryType === 'immediate') {
      // 即時配信
      await sendCampaign(campaign.id);
    } else if (deliveryType === 'scheduled') {
      // スケジュール配信
      schedulerService.scheduleAt(campaign.id, scheduledAt);
    }
    // recurring の場合は scheduler が自動的に処理
    
    res.json({ campaign });
  } catch (error) {
    console.error('Create campaign error:', error);
    res.status(500).json({ error: 'Failed to create campaign' });
  }
});

// キャンペーン一覧
app.get('/api/sites/:siteId/campaigns', authenticateToken, async (req, res) => {
  try {
    const { siteId } = req.params;
    
    const result = await pool.query(`
      SELECT c.*, 
        COUNT(d.id) FILTER (WHERE d.status = 'sent') as sent_count,
        COUNT(d.id) FILTER (WHERE d.status = 'clicked') as click_count
      FROM campaigns c
      LEFT JOIN deliveries d ON c.id = d.campaign_id
      WHERE c.site_id = $1
      GROUP BY c.id
      ORDER BY c.created_at DESC
      LIMIT 50
    `, [siteId]);
    
    res.json({ campaigns: result.rows });
  } catch (error) {
    console.error('Get campaigns error:', error);
    res.status(500).json({ error: 'Failed to get campaigns' });
  }
});

// キャンペーンキャンセル
app.post('/api/campaigns/:id/cancel', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    // ステータス確認
    const checkResult = await pool.query(
      'SELECT status, delivery_type FROM campaigns WHERE id = $1',
      [id]
    );
    
    if (checkResult.rows.length === 0) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    
    const campaign = checkResult.rows[0];
    
    if (campaign.status !== 'scheduled' && campaign.status !== 'active') {
      return res.status(400).json({ error: 'Only scheduled or active campaigns can be cancelled' });
    }
    
    // スケジューラーからキャンセル
    if (campaign.delivery_type === 'scheduled') {
      schedulerService.cancelScheduled(id);
    }
    
    // ステータス更新
    await pool.query(
      'UPDATE campaigns SET status = $1 WHERE id = $2',
      ['cancelled', id]
    );
    
    res.json({ success: true, message: 'Campaign cancelled' });
  } catch (error) {
    console.error('Cancel campaign error:', error);
    res.status(500).json({ error: 'Failed to cancel campaign' });
  }
});

// キャンペーン編集
app.put('/api/campaigns/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const {
      name, title, body, url, iconUrl, imageUrl,
      scheduledAt, recurringSchedule
    } = req.body;
    
    // scheduled または active のみ編集可能
    const checkResult = await pool.query(
      'SELECT status, delivery_type FROM campaigns WHERE id = $1',
      [id]
    );
    
    if (checkResult.rows.length === 0) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    
    const campaign = checkResult.rows[0];
    
    if (!['scheduled', 'active'].includes(campaign.status)) {
      return res.status(400).json({ error: 'Cannot edit this campaign' });
    }
    
    // 更新
    const result = await pool.query(`
      UPDATE campaigns
      SET name = $1, title = $2, body = $3, url = $4,
          icon_url = $5, image_url = $6, scheduled_at = $7,
          recurring_schedule = $8, updated_at = CURRENT_TIMESTAMP
      WHERE id = $9
      RETURNING *
    `, [
      name, title, body, url, iconUrl, imageUrl, 
      scheduledAt, recurringSchedule ? JSON.stringify(recurringSchedule) : null,
      id
    ]);
    
    // スケジュール再設定
    if (campaign.delivery_type === 'scheduled' && scheduledAt) {
      schedulerService.cancelScheduled(id);
      schedulerService.scheduleAt(id, scheduledAt);
    }
    
    res.json({ campaign: result.rows[0] });
  } catch (error) {
    console.error('Update campaign error:', error);
    res.status(500).json({ error: 'Failed to update campaign' });
  }
});

// スケジューラーステータス取得
app.get('/api/scheduler/status', authenticateToken, async (req, res) => {
  try {
    const status = schedulerService.getStatus();
    res.json(status);
  } catch (error) {
    console.error('Get scheduler status error:', error);
    res.status(500).json({ error: 'Failed to get scheduler status' });
  }
});

// ==================== レポートAPI ====================

// PDFレポート生成
app.post('/api/reports/pdf', authenticateToken, async (req, res) => {
  try {
    const { siteId, startDate, endDate } = req.body;
    
    if (!siteId || !startDate || !endDate) {
      return res.status(400).json({ error: 'siteId, startDate, and endDate are required' });
    }

    // 権限チェック
    const permissionResult = await pool.query(
      'SELECT role FROM site_permissions WHERE user_id = $1 AND site_id = $2',
      [req.user.userId, siteId]
    );

    if (permissionResult.rows.length === 0) {
      return res.status(403).json({ error: 'No permission to access this site' });
    }

    // レポート生成
    const report = await reportService.generateDashboardReport(siteId, startDate, endDate);
    
    res.json({
      success: true,
      report: {
        filename: report.filename,
        url: report.url
      }
    });
  } catch (error) {
    console.error('Generate PDF report error:', error);
    res.status(500).json({ error: 'Failed to generate report' });
  }
});

// CSVレポート生成
app.post('/api/reports/csv', authenticateToken, async (req, res) => {
  try {
    const { siteId, startDate, endDate, reportType } = req.body;
    
    if (!siteId || !startDate || !endDate) {
      return res.status(400).json({ error: 'siteId, startDate, and endDate are required' });
    }

    // 権限チェック
    const permissionResult = await pool.query(
      'SELECT role FROM site_permissions WHERE user_id = $1 AND site_id = $2',
      [req.user.userId, siteId]
    );

    if (permissionResult.rows.length === 0) {
      return res.status(403).json({ error: 'No permission to access this site' });
    }

    // CSV生成
    const report = await reportService.generateCSVReport(
      siteId, 
      startDate, 
      endDate, 
      reportType || 'campaigns'
    );
    
    res.json({
      success: true,
      report: {
        filename: report.filename,
        url: report.url
      }
    });
  } catch (error) {
    console.error('Generate CSV report error:', error);
    res.status(500).json({ error: 'Failed to generate CSV' });
  }
});

// レポートファイルダウンロード
app.get('/api/reports/:filename', authenticateToken, async (req, res) => {
  try {
    const { filename } = req.params;
    const filepath = path.join(reportService.reportsDir, filename);

    if (!fs.existsSync(filepath)) {
      return res.status(404).json({ error: 'Report not found' });
    }

    // ファイル名からsiteIdを抽出して権限チェック（簡易版）
    // 本番環境では、レポートテーブルを作成して権限管理を強化

    const ext = path.extname(filename);
    const contentType = ext === '.pdf' ? 'application/pdf' : 'text/csv';

    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    
    const fileStream = fs.createReadStream(filepath);
    fileStream.pipe(res);
  } catch (error) {
    console.error('Download report error:', error);
    res.status(500).json({ error: 'Failed to download report' });
  }
});

// レポート一覧取得
app.get('/api/reports', authenticateToken, async (req, res) => {
  try {
    const files = fs.readdirSync(reportService.reportsDir);
    
    const reports = files.map(filename => {
      const filepath = path.join(reportService.reportsDir, filename);
      const stats = fs.statSync(filepath);
      
      return {
        filename,
        size: stats.size,
        createdAt: stats.birthtime,
        url: `/api/reports/${filename}`
      };
    }).sort((a, b) => b.createdAt - a.createdAt);

    res.json({ reports });
  } catch (error) {
    console.error('Get reports error:', error);
    res.status(500).json({ error: 'Failed to get reports' });
  }
});

// 古いレポート削除
app.post('/api/reports/cleanup', authenticateToken, async (req, res) => {
  try {
    const { daysOld } = req.body;
    reportService.cleanupOldReports(daysOld || 30);
    res.json({ success: true, message: 'Old reports cleaned up' });
  } catch (error) {
    console.error('Cleanup reports error:', error);
    res.status(500).json({ error: 'Failed to cleanup reports' });
  }
});

// ==================== メールレポートAPI ====================

// メールでレポート送信
app.post('/api/reports/email', authenticateToken, async (req, res) => {
  try {
    const { siteId, recipients, startDate, endDate, subject, includeCSV } = req.body;
    
    if (!siteId || !recipients || !startDate || !endDate) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // 権限チェック
    const permissionResult = await pool.query(
      'SELECT role FROM site_permissions WHERE user_id = $1 AND site_id = $2',
      [req.user.userId, siteId]
    );

    if (permissionResult.rows.length === 0) {
      return res.status(403).json({ error: 'No permission to access this site' });
    }

    // メール送信
    const result = await emailReportService.sendReport({
      siteId,
      recipients: Array.isArray(recipients) ? recipients : [recipients],
      startDate,
      endDate,
      subject,
      includeCSV
    });

    res.json(result);
  } catch (error) {
    console.error('Email report error:', error);
    res.status(500).json({ error: 'Failed to send email report' });
  }
});

// 自動レポート設定の作成・更新
app.post('/api/reports/auto', authenticateToken, async (req, res) => {
  try {
    const {
      id,
      siteId,
      recipients,
      schedule,
      dayOfWeek,
      dayOfMonth,
      hour,
      minute,
      includeCSV
    } = req.body;

    if (!siteId || !recipients || !schedule) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // 権限チェック
    const permissionResult = await pool.query(
      'SELECT role FROM site_permissions WHERE user_id = $1 AND site_id = $2',
      [req.user.userId, siteId]
    );

    if (permissionResult.rows.length === 0) {
      return res.status(403).json({ error: 'No permission to access this site' });
    }

    // 自動レポート登録
    const autoReport = await emailReportService.scheduleAutoReport({
      id,
      siteId,
      recipients: Array.isArray(recipients) ? recipients : [recipients],
      schedule,
      dayOfWeek,
      dayOfMonth,
      hour: hour || 9,
      minute: minute || 0,
      includeCSV: includeCSV || false
    });

    res.json({ success: true, autoReport });
  } catch (error) {
    console.error('Create auto report error:', error);
    res.status(500).json({ error: 'Failed to create auto report' });
  }
});

// 自動レポート一覧取得
app.get('/api/reports/auto/:siteId', authenticateToken, async (req, res) => {
  try {
    const { siteId } = req.params;

    // 権限チェック
    const permissionResult = await pool.query(
      'SELECT role FROM site_permissions WHERE user_id = $1 AND site_id = $2',
      [req.user.userId, siteId]
    );

    if (permissionResult.rows.length === 0) {
      return res.status(403).json({ error: 'No permission to access this site' });
    }

    const autoReports = await emailReportService.listAutoReports(siteId);
    res.json({ autoReports });
  } catch (error) {
    console.error('Get auto reports error:', error);
    res.status(500).json({ error: 'Failed to get auto reports' });
  }
});

// 自動レポート停止
app.post('/api/reports/auto/:id/stop', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    await emailReportService.stopAutoReport(id);
    res.json({ success: true, message: 'Auto report stopped' });
  } catch (error) {
    console.error('Stop auto report error:', error);
    res.status(500).json({ error: 'Failed to stop auto report' });
  }
});

// テストメール送信
app.post('/api/reports/test-email', authenticateToken, async (req, res) => {
  try {
    const { email } = req.body;
    
    if (!email) {
      return res.status(400).json({ error: 'Email is required' });
    }

    const result = await emailReportService.sendTestEmail(email);
    res.json(result);
  } catch (error) {
    console.error('Test email error:', error);
    res.status(500).json({ error: 'Failed to send test email' });
  }
});

// SMTP接続確認
app.get('/api/reports/smtp/verify', authenticateToken, async (req, res) => {
  try {
    const result = await emailReportService.verifyConnection();
    res.json(result);
  } catch (error) {
    console.error('SMTP verify error:', error);
    res.status(500).json({ error: 'Failed to verify SMTP connection' });
  }
});

// ==================== レポートテンプレートAPI ====================

// カスタムテンプレート取得
app.get('/api/reports/template/:siteId', authenticateToken, async (req, res) => {
  try {
    const { siteId } = req.params;
    const template = await reportService.getCustomTemplate(siteId);
    res.json({ template });
  } catch (error) {
    console.error('Get template error:', error);
    res.status(500).json({ error: 'Failed to get template' });
  }
});

// カスタムテンプレート保存
app.post('/api/reports/template/:siteId', authenticateToken, async (req, res) => {
  try {
    const { siteId } = req.params;
    const { template } = req.body;

    // 権限チェック
    const permissionResult = await pool.query(
      'SELECT role FROM site_permissions WHERE user_id = $1 AND site_id = $2',
      [req.user.userId, siteId]
    );

    if (permissionResult.rows.length === 0) {
      return res.status(403).json({ error: 'No permission to access this site' });
    }

    await reportService.saveCustomTemplate(siteId, template);
    res.json({ success: true, message: 'Template saved' });
  } catch (error) {
    console.error('Save template error:', error);
    res.status(500).json({ error: 'Failed to save template' });
  }
});

// ロゴアップロード
app.post('/api/reports/logo/:siteId', authenticateToken, async (req, res) => {
  try {
    const { siteId } = req.params;

    // 権限チェック
    const permissionResult = await pool.query(
      'SELECT role FROM site_permissions WHERE user_id = $1 AND site_id = $2',
      [req.user.userId, siteId]
    );

    if (permissionResult.rows.length === 0) {
      return res.status(403).json({ error: 'No permission to access this site' });
    }

    // マルチパートフォーム対応が必要（multerなど）
    // 簡易版として base64 を想定
    const { logoBase64 } = req.body;
    
    if (!logoBase64) {
      return res.status(400).json({ error: 'Logo data is required' });
    }

    const logoBuffer = Buffer.from(logoBase64, 'base64');
    const result = await reportService.uploadLogo(siteId, logoBuffer);
    
    res.json(result);
  } catch (error) {
    console.error('Upload logo error:', error);
    res.status(500).json({ error: 'Failed to upload logo' });
  }
});

// ==================== 配信処理 ====================

async function sendCampaign(campaignId) {
  const client = await pool.connect();
  
  try {
    // キャンペーン情報取得
    const campaignResult = await client.query(`
      SELECT c.*, s.vapid_public_key, s.vapid_private_key
      FROM campaigns c
      INNER JOIN sites s ON c.site_id = s.id
      WHERE c.id = $1
    `, [campaignId]);
    
    if (campaignResult.rows.length === 0) {
      throw new Error('Campaign not found');
    }
    
    const campaign = campaignResult.rows[0];
    
    // VAPID設定
    webpush.setVapidDetails(
      'mailto:noreply@example.com',
      campaign.vapid_public_key,
      campaign.vapid_private_key
    );
    
    // 購読者取得（セグメント考慮）
    let query = `
      SELECT id, endpoint, p256dh_key, auth_key
      FROM subscribers
      WHERE site_id = $1 AND is_active = true
    `;
    const params = [campaign.site_id];
    
    if (campaign.segment_id) {
      // セグメントフィルタリング（実装簡略化）
      query += ` AND id IN (SELECT subscriber_id FROM segment_members WHERE segment_id = $2)`;
      params.push(campaign.segment_id);
    }
    
    const subscribersResult = await client.query(query, params);
    const subscribers = subscribersResult.rows;
    
    // ペイロード作成
    const payload = JSON.stringify({
      title: campaign.title,
      body: campaign.body,
      icon: campaign.icon_url,
      image: campaign.image_url,
      url: campaign.url,
      campaignId: campaign.id,
      deliveryId: null // 各配信で設定
    });
    
    // 各購読者に配信
    const deliveryPromises = subscribers.map(async (subscriber) => {
      try {
        // 配信ログ作成
        const deliveryResult = await client.query(`
          INSERT INTO deliveries (campaign_id, subscriber_id, status)
          VALUES ($1, $2, 'queued')
          RETURNING id
        `, [campaignId, subscriber.id]);
        
        const deliveryId = deliveryResult.rows[0].id;
        
        // ペイロードにdeliveryId追加
        const customPayload = JSON.parse(payload);
        customPayload.deliveryId = deliveryId;
        
        // プッシュ送信
        await webpush.sendNotification(
          {
            endpoint: subscriber.endpoint,
            keys: {
              p256dh: subscriber.p256dh_key,
              auth: subscriber.auth_key
            }
          },
          JSON.stringify(customPayload)
        );
        
        // 成功ステータス更新
        await client.query(`
          UPDATE deliveries
          SET status = 'sent', sent_at = CURRENT_TIMESTAMP
          WHERE id = $1
        `, [deliveryId]);
        
        return { success: true, deliveryId };
      } catch (error) {
        console.error('Send notification error:', error);
        
        // エラーステータス更新
        await client.query(`
          UPDATE deliveries
          SET status = 'failed', error_message = $1
          WHERE campaign_id = $2 AND subscriber_id = $3
        `, [error.message, campaignId, subscriber.id]);
        
        // 410エラー（購読期限切れ）の場合は購読を無効化
        if (error.statusCode === 410) {
          await client.query(`
            UPDATE subscribers
            SET is_active = false
            WHERE id = $1
          `, [subscriber.id]);
        }
        
        return { success: false, error: error.message };
      }
    });
    
    await Promise.all(deliveryPromises);
    
    // キャンペーンステータス更新
    await client.query(`
      UPDATE campaigns
      SET status = 'completed'
      WHERE id = $1
    `, [campaignId]);
    
  } catch (error) {
    console.error('Send campaign error:', error);
    throw error;
  } finally {
    client.release();
  }
}

// クリック追跡
app.post('/api/track-click', async (req, res) => {
  try {
    const { deliveryId, clickedAt } = req.body;
    
    await pool.query(`
      UPDATE deliveries
      SET status = 'clicked', clicked_at = $1
      WHERE id = $2
    `, [clickedAt, deliveryId]);
    
    res.json({ success: true });
  } catch (error) {
    console.error('Track click error:', error);
    res.status(500).json({ error: 'Failed to track click' });
  }
});

// 閉じる追跡
app.post('/api/track-close', async (req, res) => {
  try {
    const { deliveryId, closedAt } = req.body;
    
    await pool.query(`
      UPDATE deliveries
      SET closed_at = $1
      WHERE id = $2
    `, [closedAt, deliveryId]);
    
    res.json({ success: true });
  } catch (error) {
    console.error('Track close error:', error);
    res.status(500).json({ error: 'Failed to track close' });
  }
});

// ==================== 統計API ====================

// ダッシュボード統計
app.get('/api/sites/:siteId/stats', authenticateToken, async (req, res) => {
  try {
    const { siteId } = req.params;
    
    const result = await pool.query(`
      SELECT 
        COUNT(DISTINCT s.id) FILTER (WHERE s.is_active = true) as active_subscribers,
        COUNT(DISTINCT c.id) as total_campaigns,
        SUM(CASE WHEN d.status = 'sent' THEN 1 ELSE 0 END) as total_sent,
        SUM(CASE WHEN d.status = 'clicked' THEN 1 ELSE 0 END) as total_clicks
      FROM sites site
      LEFT JOIN subscribers s ON site.id = s.site_id
      LEFT JOIN campaigns c ON site.id = c.site_id
      LEFT JOIN deliveries d ON c.id = d.campaign_id
      WHERE site.id = $1
    `, [siteId]);
    
    res.json({ stats: result.rows[0] });
  } catch (error) {
    console.error('Get stats error:', error);
    res.status(500).json({ error: 'Failed to get stats' });
  }
});

// サーバー起動
app.listen(PORT, () => {
  console.log(`Web Push API server running on port ${PORT}`);
  
  // スケジューラーサービス起動
  schedulerService.start();
  
  // 自動レポートサービス起動
  emailReportService.loadAndStartAutoReports();
});