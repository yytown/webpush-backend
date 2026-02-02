// スケジュール配信サービス
// Node.js + node-cron

const cron = require('node-cron');
const { Pool } = require('pg');
const webpush = require('web-push');

// PostgreSQL接続
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'webpush',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'password',
});

class SchedulerService {
  constructor() {
    this.scheduledJobs = new Map();
    this.isRunning = false;
  }

  // スケジューラー開始
  start() {
    if (this.isRunning) {
      console.log('Scheduler is already running');
      return;
    }

    console.log('Starting scheduler service...');
    this.isRunning = true;

    // 1分ごとにスケジュール済みキャンペーンをチェック
    this.mainTask = cron.schedule('* * * * *', async () => {
      await this.checkScheduledCampaigns();
    });

    // 起動時に一度実行
    this.checkScheduledCampaigns();

    // 繰り返し配信のチェック（5分ごと）
    this.recurringTask = cron.schedule('*/5 * * * *', async () => {
      await this.checkRecurringCampaigns();
    });

    console.log('Scheduler service started successfully');
  }

  // スケジューラー停止
  stop() {
    if (this.mainTask) {
      this.mainTask.stop();
    }
    if (this.recurringTask) {
      this.recurringTask.stop();
    }
    
    // 個別ジョブも停止
    this.scheduledJobs.forEach(job => job.stop());
    this.scheduledJobs.clear();
    
    this.isRunning = false;
    console.log('Scheduler service stopped');
  }

  // スケジュール済みキャンペーンをチェック
  async checkScheduledCampaigns() {
    const client = await pool.connect();
    
    try {
      // 配信時刻を過ぎたスケジュール済みキャンペーンを取得
      const result = await client.query(`
        SELECT id, site_id, scheduled_at
        FROM campaigns
        WHERE status = 'scheduled'
          AND delivery_type = 'scheduled'
          AND scheduled_at <= NOW()
        ORDER BY scheduled_at ASC
        LIMIT 50
      `);

      const campaigns = result.rows;

      if (campaigns.length > 0) {
        console.log(`Found ${campaigns.length} campaigns to send`);
      }

      for (const campaign of campaigns) {
        try {
          // ステータスを配信中に更新
          await client.query(`
            UPDATE campaigns
            SET status = 'sending'
            WHERE id = $1
          `, [campaign.id]);

          // 配信実行
          await this.sendCampaign(campaign.id);

          console.log(`Campaign ${campaign.id} sent successfully`);
        } catch (error) {
          console.error(`Failed to send campaign ${campaign.id}:`, error);
          
          // エラー時はステータスを失敗に
          await client.query(`
            UPDATE campaigns
            SET status = 'failed'
            WHERE id = $1
          `, [campaign.id]);
        }
      }
    } catch (error) {
      console.error('Error checking scheduled campaigns:', error);
    } finally {
      client.release();
    }
  }

  // 繰り返し配信のチェック
  async checkRecurringCampaigns() {
    const client = await pool.connect();
    
    try {
      // アクティブな繰り返し配信を取得
      const result = await client.query(`
        SELECT id, site_id, recurring_schedule
        FROM campaigns
        WHERE status = 'active'
          AND delivery_type = 'recurring'
          AND recurring_schedule IS NOT NULL
      `);

      const campaigns = result.rows;

      for (const campaign of campaigns) {
        const schedule = campaign.recurring_schedule;
        
        // 次の配信時刻を判定
        if (this.shouldSendNow(schedule)) {
          try {
            console.log(`Sending recurring campaign ${campaign.id}`);
            await this.sendCampaign(campaign.id);
            
            // 最終配信時刻を記録
            await client.query(`
              UPDATE campaigns
              SET recurring_schedule = jsonb_set(
                recurring_schedule,
                '{last_sent}',
                to_jsonb(NOW())
              )
              WHERE id = $1
            `, [campaign.id]);
          } catch (error) {
            console.error(`Failed to send recurring campaign ${campaign.id}:`, error);
          }
        }
      }
    } catch (error) {
      console.error('Error checking recurring campaigns:', error);
    } finally {
      client.release();
    }
  }

  // 繰り返し配信の送信判定
  shouldSendNow(schedule) {
    const now = new Date();
    const lastSent = schedule.last_sent ? new Date(schedule.last_sent) : null;

    // 頻度チェック
    if (schedule.frequency === 'daily') {
      // 毎日指定時刻
      const targetHour = schedule.hour || 10;
      const targetMinute = schedule.minute || 0;
      
      if (now.getHours() === targetHour && now.getMinutes() === targetMinute) {
        // 今日まだ送信していない
        if (!lastSent || lastSent.toDateString() !== now.toDateString()) {
          return true;
        }
      }
    } else if (schedule.frequency === 'weekly') {
      // 毎週指定曜日・時刻
      const targetDayOfWeek = schedule.day_of_week || 1; // 0=日曜
      const targetHour = schedule.hour || 10;
      const targetMinute = schedule.minute || 0;
      
      if (now.getDay() === targetDayOfWeek &&
          now.getHours() === targetHour &&
          now.getMinutes() === targetMinute) {
        // 今週まだ送信していない
        const weekStart = new Date(now);
        weekStart.setDate(now.getDate() - now.getDay());
        weekStart.setHours(0, 0, 0, 0);
        
        if (!lastSent || lastSent < weekStart) {
          return true;
        }
      }
    } else if (schedule.frequency === 'monthly') {
      // 毎月指定日・時刻
      const targetDay = schedule.day || 1;
      const targetHour = schedule.hour || 10;
      const targetMinute = schedule.minute || 0;
      
      if (now.getDate() === targetDay &&
          now.getHours() === targetHour &&
          now.getMinutes() === targetMinute) {
        // 今月まだ送信していない
        if (!lastSent || 
            lastSent.getMonth() !== now.getMonth() ||
            lastSent.getFullYear() !== now.getFullYear()) {
          return true;
        }
      }
    } else if (schedule.frequency === 'interval') {
      // インターバル（X分/時間/日ごと）
      if (!lastSent) return true;
      
      const intervalMs = this.getIntervalMilliseconds(schedule.interval_value, schedule.interval_unit);
      const timeSinceLastSent = now.getTime() - new Date(lastSent).getTime();
      
      if (timeSinceLastSent >= intervalMs) {
        return true;
      }
    }

    return false;
  }

  // インターバルをミリ秒に変換
  getIntervalMilliseconds(value, unit) {
    const multipliers = {
      'minutes': 60 * 1000,
      'hours': 60 * 60 * 1000,
      'days': 24 * 60 * 60 * 1000
    };
    return value * (multipliers[unit] || multipliers.hours);
  }

  // キャンペーン配信（backend APIから移植）
  async sendCampaign(campaignId) {
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
      
      // 購読者取得
      let query = `
        SELECT id, endpoint, p256dh_key, auth_key
        FROM subscribers
        WHERE site_id = $1 AND is_active = true
      `;
      const params = [campaign.site_id];
      
      if (campaign.segment_id) {
        query += ` AND id IN (SELECT subscriber_id FROM segment_members WHERE segment_id = $2)`;
        params.push(campaign.segment_id);
      }
      
      const subscribersResult = await client.query(query, params);
      const subscribers = subscribersResult.rows;
      
      console.log(`Sending to ${subscribers.length} subscribers`);
      
      // ペイロード作成
      const payload = JSON.stringify({
        title: campaign.title,
        body: campaign.body,
        icon: campaign.icon_url,
        image: campaign.image_url,
        url: campaign.url,
        campaignId: campaign.id,
        deliveryId: null
      });
      
      // 配信処理（並列実行、バッチサイズ50）
      const batchSize = 50;
      let successCount = 0;
      let failureCount = 0;
      
      for (let i = 0; i < subscribers.length; i += batchSize) {
        const batch = subscribers.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (subscriber) => {
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
            
            successCount++;
          } catch (error) {
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
            
            failureCount++;
          }
        });
        
        await Promise.all(batchPromises);
        
        // 進捗表示
        console.log(`Progress: ${Math.min(i + batchSize, subscribers.length)}/${subscribers.length}`);
      }
      
      // キャンペーンステータス更新
      const finalStatus = campaign.delivery_type === 'recurring' ? 'active' : 'completed';
      await client.query(`
        UPDATE campaigns
        SET status = $1
        WHERE id = $2
      `, [finalStatus, campaignId]);
      
      // 統計集計
      await this.updateCampaignStats(campaignId);
      
      console.log(`Campaign ${campaignId} completed: ${successCount} success, ${failureCount} failed`);
      
    } catch (error) {
      console.error('Send campaign error:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  // キャンペーン統計を更新
  async updateCampaignStats(campaignId) {
    const client = await pool.connect();
    
    try {
      await client.query(`
        INSERT INTO campaign_stats (campaign_id, date, sent_count, failed_count, clicked_count, unique_clicks)
        SELECT 
          campaign_id,
          CURRENT_DATE,
          COUNT(*) FILTER (WHERE status = 'sent'),
          COUNT(*) FILTER (WHERE status = 'failed'),
          COUNT(*) FILTER (WHERE status = 'clicked'),
          COUNT(DISTINCT subscriber_id) FILTER (WHERE status = 'clicked')
        FROM deliveries
        WHERE campaign_id = $1
          AND sent_at::date = CURRENT_DATE
        GROUP BY campaign_id
        ON CONFLICT (campaign_id, date) 
        DO UPDATE SET
          sent_count = EXCLUDED.sent_count,
          failed_count = EXCLUDED.failed_count,
          clicked_count = EXCLUDED.clicked_count,
          unique_clicks = EXCLUDED.unique_clicks,
          ctr = CASE 
            WHEN EXCLUDED.sent_count > 0 
            THEN ROUND((EXCLUDED.clicked_count::decimal / EXCLUDED.sent_count) * 100, 2)
            ELSE 0
          END,
          updated_at = CURRENT_TIMESTAMP
      `, [campaignId]);
    } catch (error) {
      console.error('Update stats error:', error);
    } finally {
      client.release();
    }
  }

  // 特定の日時にキャンペーンをスケジュール
  scheduleAt(campaignId, scheduledDate) {
    const now = new Date();
    const targetDate = new Date(scheduledDate);
    
    if (targetDate <= now) {
      console.log(`Campaign ${campaignId} scheduled time has passed, sending immediately`);
      this.sendCampaign(campaignId);
      return;
    }

    // Cron式を生成（分 時 日 月 曜日）
    const minute = targetDate.getMinutes();
    const hour = targetDate.getHours();
    const day = targetDate.getDate();
    const month = targetDate.getMonth() + 1;
    
    const cronExpression = `${minute} ${hour} ${day} ${month} *`;
    
    console.log(`Scheduling campaign ${campaignId} at ${scheduledDate} (cron: ${cronExpression})`);
    
    // ジョブをスケジュール
    const job = cron.schedule(cronExpression, async () => {
      console.log(`Executing scheduled campaign ${campaignId}`);
      await this.sendCampaign(campaignId);
      
      // 1回きりなのでジョブを停止
      job.stop();
      this.scheduledJobs.delete(campaignId);
    });
    
    this.scheduledJobs.set(campaignId, job);
  }

  // スケジュールされたキャンペーンをキャンセル
  cancelScheduled(campaignId) {
    if (this.scheduledJobs.has(campaignId)) {
      this.scheduledJobs.get(campaignId).stop();
      this.scheduledJobs.delete(campaignId);
      console.log(`Cancelled scheduled campaign ${campaignId}`);
      return true;
    }
    return false;
  }

  // 統計情報を取得
  getStatus() {
    return {
      isRunning: this.isRunning,
      scheduledJobsCount: this.scheduledJobs.size,
      scheduledJobs: Array.from(this.scheduledJobs.keys())
    };
  }
}

// シングルトンインスタンス
const schedulerService = new SchedulerService();

// プロセス終了時にクリーンアップ
process.on('SIGTERM', () => {
  console.log('SIGTERM received, stopping scheduler...');
  schedulerService.stop();
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('SIGINT received, stopping scheduler...');
  schedulerService.stop();
  process.exit(0);
});

module.exports = schedulerService;

// スタンドアロン実行
if (require.main === module) {
  console.log('Starting scheduler service as standalone process...');
  schedulerService.start();
}