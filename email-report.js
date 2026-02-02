// メールレポートサービス
// Nodemailer + スケジューラー

const nodemailer = require('nodemailer');
const cron = require('node-cron');
const { Pool } = require('pg');
const reportService = require('./report');
const path = require('path');

// PostgreSQL接続
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'webpush',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'password',
});

class EmailReportService {
  constructor() {
    this.transporter = this.createTransporter();
    this.scheduledJobs = new Map();
  }

  // メールトランスポーター作成
  createTransporter() {
    return nodemailer.createTransport({
      host: process.env.SMTP_HOST || 'smtp.gmail.com',
      port: process.env.SMTP_PORT || 587,
      secure: false, // TLS
      auth: {
        user: process.env.SMTP_USER,
        pass: process.env.SMTP_PASS
      }
    });
  }

  // 単発メール送信
  async sendReport(options) {
    const {
      siteId,
      recipients,
      startDate,
      endDate,
      subject,
      includeCSV = false
    } = options;

    try {
      // サイト情報取得
      const siteResult = await pool.query(
        'SELECT name, url FROM sites WHERE id = $1',
        [siteId]
      );

      if (siteResult.rows.length === 0) {
        throw new Error('Site not found');
      }

      const site = siteResult.rows[0];

      // PDFレポート生成
      const pdfReport = await reportService.generateDashboardReport(
        siteId,
        startDate,
        endDate
      );

      const attachments = [
        {
          filename: pdfReport.filename,
          path: pdfReport.filepath
        }
      ];

      // CSV含める場合
      if (includeCSV) {
        const csvReport = await reportService.generateCSVReport(
          siteId,
          startDate,
          endDate,
          'campaigns'
        );
        attachments.push({
          filename: csvReport.filename,
          path: csvReport.filepath
        });
      }

      // メール本文生成
      const htmlBody = this.generateEmailHTML(site, startDate, endDate);

      // メール送信
      const info = await this.transporter.sendMail({
        from: `"WebPush Pro" <${process.env.SMTP_USER}>`,
        to: recipients.join(', '),
        subject: subject || `${site.name} - Webプッシュ通知レポート (${startDate} - ${endDate})`,
        html: htmlBody,
        attachments
      });

      console.log('Report email sent:', info.messageId);

      return {
        success: true,
        messageId: info.messageId,
        recipients: recipients
      };
    } catch (error) {
      console.error('Send report email error:', error);
      throw error;
    }
  }

  // メール本文HTML生成
  generateEmailHTML(site, startDate, endDate) {
    return `
      <!DOCTYPE html>
      <html lang="ja">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
          body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
          }
          .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            text-align: center;
            margin-bottom: 30px;
          }
          .header h1 {
            margin: 0 0 10px 0;
            font-size: 24px;
          }
          .header p {
            margin: 0;
            opacity: 0.9;
          }
          .content {
            background: #f7fafc;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 20px;
          }
          .content h2 {
            color: #667eea;
            margin-top: 0;
          }
          .info-box {
            background: white;
            padding: 15px;
            border-radius: 8px;
            margin: 15px 0;
            border-left: 4px solid #667eea;
          }
          .info-box strong {
            color: #667eea;
          }
          .button {
            display: inline-block;
            background: #667eea;
            color: white;
            padding: 12px 30px;
            text-decoration: none;
            border-radius: 8px;
            margin: 20px 0;
          }
          .footer {
            text-align: center;
            color: #999;
            font-size: 12px;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eee;
          }
          .attachment-info {
            background: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 8px;
            padding: 15px;
            margin: 20px 0;
          }
        </style>
      </head>
      <body>
        <div class="header">
          <h1>📊 Webプッシュ通知レポート</h1>
          <p>${site.name}</p>
        </div>

        <div class="content">
          <h2>レポート準備完了</h2>
          <p>こんにちは、</p>
          <p>以下の期間のWebプッシュ通知レポートが準備できました。</p>

          <div class="info-box">
            <strong>対象サイト:</strong> ${site.name}<br>
            <strong>URL:</strong> ${site.url}<br>
            <strong>期間:</strong> ${startDate} - ${endDate}
          </div>

          <div class="attachment-info">
            <strong>📎 添付ファイル:</strong><br>
            • PDFレポート - グラフと統計を含む詳細レポート<br>
            • CSVデータ（オプション） - Excelで分析可能なデータ
          </div>

          <p>添付のPDFファイルには以下の情報が含まれています：</p>
          <ul>
            <li>購読者数と配信統計</li>
            <li>日別トレンドグラフ</li>
            <li>トップパフォーマンスキャンペーン</li>
            <li>デバイス・ブラウザ分布</li>
          </ul>

          <a href="${site.url}" class="button">ダッシュボードを見る</a>
        </div>

        <div class="footer">
          <p>このメールは自動送信されています。</p>
          <p>© 2026 WebPush Pro. All rights reserved.</p>
        </div>
      </body>
      </html>
    `;
  }

  // 自動レポート設定の登録
  async scheduleAutoReport(options) {
    const {
      id,
      siteId,
      recipients,
      schedule, // 'daily', 'weekly', 'monthly'
      dayOfWeek, // 0-6 (週次の場合)
      dayOfMonth, // 1-31 (月次の場合)
      hour,
      minute,
      includeCSV
    } = options;

    try {
      // データベースに保存
      const result = await pool.query(`
        INSERT INTO auto_reports (
          id, site_id, recipients, schedule_type, 
          day_of_week, day_of_month, hour, minute, include_csv, is_active
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, true)
        ON CONFLICT (id) DO UPDATE SET
          recipients = $3,
          schedule_type = $4,
          day_of_week = $5,
          day_of_month = $6,
          hour = $7,
          minute = $8,
          include_csv = $9,
          is_active = true
        RETURNING *
      `, [
        id || crypto.randomUUID(),
        siteId,
        JSON.stringify(recipients),
        schedule,
        dayOfWeek,
        dayOfMonth,
        hour,
        minute,
        includeCSV
      ]);

      const autoReport = result.rows[0];

      // Cronジョブをスケジュール
      this.registerCronJob(autoReport);

      return autoReport;
    } catch (error) {
      console.error('Schedule auto report error:', error);
      throw error;
    }
  }

  // Cronジョブ登録
  registerCronJob(autoReport) {
    const { id, schedule_type, day_of_week, day_of_month, hour, minute } = autoReport;

    // 既存のジョブがあれば停止
    if (this.scheduledJobs.has(id)) {
      this.scheduledJobs.get(id).stop();
    }

    let cronExpression;

    if (schedule_type === 'daily') {
      // 毎日 HH:MM
      cronExpression = `${minute} ${hour} * * *`;
    } else if (schedule_type === 'weekly') {
      // 毎週X曜日 HH:MM
      cronExpression = `${minute} ${hour} * * ${day_of_week}`;
    } else if (schedule_type === 'monthly') {
      // 毎月X日 HH:MM
      cronExpression = `${minute} ${hour} ${day_of_month} * *`;
    }

    console.log(`Scheduling auto report ${id}: ${cronExpression}`);

    const job = cron.schedule(cronExpression, async () => {
      await this.executeAutoReport(autoReport);
    });

    this.scheduledJobs.set(id, job);
  }

  // 自動レポート実行
  async executeAutoReport(autoReport) {
    try {
      console.log(`Executing auto report: ${autoReport.id}`);

      // 期間計算
      const { startDate, endDate } = this.calculateReportPeriod(autoReport.schedule_type);

      // メール送信
      await this.sendReport({
        siteId: autoReport.site_id,
        recipients: JSON.parse(autoReport.recipients),
        startDate,
        endDate,
        includeCSV: autoReport.include_csv
      });

      // 最終実行日時を更新
      await pool.query(
        'UPDATE auto_reports SET last_sent_at = NOW() WHERE id = $1',
        [autoReport.id]
      );

      console.log(`Auto report ${autoReport.id} sent successfully`);
    } catch (error) {
      console.error(`Failed to execute auto report ${autoReport.id}:`, error);
    }
  }

  // レポート期間計算
  calculateReportPeriod(scheduleType) {
    const now = new Date();
    let startDate, endDate;

    if (scheduleType === 'daily') {
      // 前日
      const yesterday = new Date(now);
      yesterday.setDate(yesterday.getDate() - 1);
      startDate = endDate = yesterday.toISOString().split('T')[0];
    } else if (scheduleType === 'weekly') {
      // 先週（月曜〜日曜）
      const lastMonday = new Date(now);
      lastMonday.setDate(lastMonday.getDate() - lastMonday.getDay() - 6);
      const lastSunday = new Date(lastMonday);
      lastSunday.setDate(lastSunday.getDate() + 6);

      startDate = lastMonday.toISOString().split('T')[0];
      endDate = lastSunday.toISOString().split('T')[0];
    } else if (scheduleType === 'monthly') {
      // 先月
      const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const lastMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0);

      startDate = lastMonth.toISOString().split('T')[0];
      endDate = lastMonthEnd.toISOString().split('T')[0];
    }

    return { startDate, endDate };
  }

  // すべての自動レポートを読み込んで開始
  async loadAndStartAutoReports() {
    try {
      const result = await pool.query(
        'SELECT * FROM auto_reports WHERE is_active = true'
      );

      const autoReports = result.rows;

      console.log(`Loading ${autoReports.length} auto reports...`);

      autoReports.forEach(autoReport => {
        this.registerCronJob(autoReport);
      });

      console.log('Auto reports loaded successfully');
    } catch (error) {
      console.error('Load auto reports error:', error);
    }
  }

  // 自動レポートの停止
  async stopAutoReport(id) {
    try {
      // Cronジョブ停止
      if (this.scheduledJobs.has(id)) {
        this.scheduledJobs.get(id).stop();
        this.scheduledJobs.delete(id);
      }

      // データベース更新
      await pool.query(
        'UPDATE auto_reports SET is_active = false WHERE id = $1',
        [id]
      );

      console.log(`Auto report ${id} stopped`);
      return { success: true };
    } catch (error) {
      console.error('Stop auto report error:', error);
      throw error;
    }
  }

  // 自動レポート一覧取得
  async listAutoReports(siteId) {
    try {
      const result = await pool.query(
        'SELECT * FROM auto_reports WHERE site_id = $1 ORDER BY created_at DESC',
        [siteId]
      );

      return result.rows.map(row => ({
        ...row,
        recipients: JSON.parse(row.recipients)
      }));
    } catch (error) {
      console.error('List auto reports error:', error);
      throw error;
    }
  }

  // テストメール送信
  async sendTestEmail(email) {
    try {
      const info = await this.transporter.sendMail({
        from: `"WebPush Pro" <${process.env.SMTP_USER}>`,
        to: email,
        subject: 'テストメール - WebPush Pro',
        html: `
          <div style="font-family: sans-serif; padding: 20px;">
            <h2 style="color: #667eea;">メール設定テスト</h2>
            <p>このメールが届いていれば、メール設定は正常に動作しています。</p>
            <p><strong>送信日時:</strong> ${new Date().toLocaleString('ja-JP')}</p>
          </div>
        `
      });

      return {
        success: true,
        messageId: info.messageId
      };
    } catch (error) {
      console.error('Send test email error:', error);
      throw error;
    }
  }

  // 接続テスト
  async verifyConnection() {
    try {
      await this.transporter.verify();
      console.log('SMTP connection verified');
      return { success: true, message: 'SMTP connection is ready' };
    } catch (error) {
      console.error('SMTP verification failed:', error);
      return { success: false, message: error.message };
    }
  }
}

// 自動レポートテーブル作成SQL
const createAutoReportsTable = `
CREATE TABLE IF NOT EXISTS auto_reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  site_id UUID NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
  recipients JSONB NOT NULL,
  schedule_type VARCHAR(20) NOT NULL, -- 'daily', 'weekly', 'monthly'
  day_of_week INT, -- 0-6
  day_of_month INT, -- 1-31
  hour INT NOT NULL, -- 0-23
  minute INT NOT NULL, -- 0-59
  include_csv BOOLEAN DEFAULT false,
  is_active BOOLEAN DEFAULT true,
  last_sent_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_auto_reports_site ON auto_reports(site_id);
CREATE INDEX IF NOT EXISTS idx_auto_reports_active ON auto_reports(is_active) WHERE is_active = true;
`;

module.exports = new EmailReportService();