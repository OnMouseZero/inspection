import sqlite3
import datetime
import logging
import os
import base64
from io import BytesIO
from collections import defaultdict
import matplotlib.pyplot as plt
from weasyprint import HTML

# ================= 配置区 =================
DB_FILE = 'alerts.db'
REPORT_FILENAME = 'weekly_inspection_report_custom.pdf'
TOP_N_ALERTS = 3

# 中文字体配置 (确保绘图不乱码)
plt.rcParams['font.sans-serif'] = ['SimHei', 'WenQuanYi Micro Hei', 'DejaVu Sans'] 
plt.rcParams['axes.unicode_minus'] = False
# =========================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def get_weekly_alerts(start_date_str=None, end_date_str=None):
    """查询指定日期范围的数据及趋势"""
    if not os.path.exists(DB_FILE):
        logging.warning(f"数据库文件 {DB_FILE} 不存在")
        return [], {}, (None, None)

    # 日期解析逻辑
    if start_date_str and end_date_str:
        start_dt = datetime.datetime.strptime(start_date_str, '%Y.%m.%d')
        end_dt = datetime.datetime.strptime(end_date_str, '%Y.%m.%d').replace(hour=23, minute=59, second=59)
    else:
        end_dt = datetime.datetime.now()
        start_dt = end_dt - datetime.timedelta(days=7)

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    start_fmt = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_fmt = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        # 1. 查询明细
        sql = """
        SELECT t1.cluster, t1.namespace, t1.alert_name, MAX(t1.level) as max_level,
               MAX(t1.metric_type), t1.target,
               (SELECT key_info FROM weekly_alerts t2 WHERE t2.cluster = t1.cluster AND t2.namespace = t1.namespace AND t2.alert_name = t1.alert_name AND t2.target = t1.target ORDER BY t2.starts_at DESC LIMIT 1),
               (SELECT detail_info FROM weekly_alerts t3 WHERE t3.cluster = t1.cluster AND t3.namespace = t1.namespace AND t3.alert_name = t1.alert_name AND t3.target = t1.target ORDER BY t3.starts_at DESC LIMIT 1),
               COUNT(*) as frequency, MIN(t1.starts_at), MAX(t1.starts_at)
        FROM weekly_alerts t1 WHERE t1.created_at BETWEEN ? AND ?
        GROUP BY t1.cluster, t1.namespace, t1.alert_name, t1.target
        ORDER BY t1.cluster ASC, max_level DESC, frequency DESC
        """
        c.execute(sql, (start_fmt, end_fmt))
        rows = c.fetchall()

        # 2. 查询每日趋势
        trend_sql = "SELECT strftime('%m-%d', created_at) as day, COUNT(*) FROM weekly_alerts WHERE created_at BETWEEN ? AND ? GROUP BY day"
        c.execute(trend_sql, (start_fmt, end_fmt))
        trend_data = dict(c.fetchall())

        return rows, trend_data, (start_dt, end_dt)
    finally:
        conn.close()

def generate_trend_chart(trend_data, start_dt, end_dt):
    """绘制趋势图"""
    days, counts = [], []
    curr = start_dt
    while curr <= end_dt:
        d_str = curr.strftime('%m-%d')
        days.append(d_str)
        counts.append(trend_data.get(d_str, 0))
        curr += datetime.timedelta(days=1)

    plt.figure(figsize=(10, 3.5))
    bars = plt.bar(days, counts, color='#3498db', alpha=0.7, width=0.5)
    plt.plot(days, counts, marker='o', color='#e74c3c', linewidth=2, markersize=4)
    plt.title('告警数量每日趋势走势', fontsize=12, pad=10)
    plt.grid(axis='y', linestyle='--', alpha=0.4)
    
    # 在柱状图上方标注数值
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1, f'{int(height)}', ha='center', va='bottom', fontsize=9)

    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    plt.close()
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def generate_html(alerts, trend_data, date_range):
    start_dt, end_dt = date_range
    systems_data = defaultdict(lambda: {'total': 0, 'levels': {4:0,3:0,2:0,1:0}, 'rows': []})
    global_levels = {4:0,3:0,2:0,1:0}
    global_alert_names = defaultdict(int)

    for row in alerts:
        cluster, alert_name, freq = row[0], row[2], row[8]
        try: level = int(row[3])
        except: level = 1
        systems_data[cluster]['rows'].append(row)
        systems_data[cluster]['total'] += freq
        systems_data[cluster]['levels'][level] += freq 
        global_levels[level] += freq
        global_alert_names[alert_name] += freq

    top_n_data = sorted(global_alert_names.items(), key=lambda x: x[1], reverse=True)[:TOP_N_ALERTS]
    chart_img = generate_trend_chart(trend_data, start_dt, end_dt)

    # CSS 样式 (保持你喜欢的精致感)
    css = """
    <style>
        @page { margin: 1cm; size: A4; }
        body { font-family: "WenQuanYi Micro Hei", sans-serif; font-size: 11px; color: #333; line-height: 1.4; }
        .report-header { text-align: center; border-bottom: 2px solid #2c3e50; padding-bottom: 10px; margin-bottom: 15px; }
        .global-summary-box { background: #f8f9fa; border: 1px solid #ddd; border-radius: 6px; padding: 12px; margin-bottom: 20px; }
        .summary-top-row { display: flex; justify-content: space-between; margin-bottom: 10px; font-weight: bold; }
        .level-card { background: #fff; padding: 5px; border-radius: 4px; border: 1px solid #e0e0e0; text-align: center; min-width: 70px; }
        .top-alerts-container { margin-bottom: 20px; border: 1px solid #ebccd1; border-radius: 4px; overflow: hidden; }
        .top-header { background: #f2dede; color: #a94442; padding: 6px 12px; font-weight: bold; }
        .top-table { width: 100%; border-collapse: collapse; }
        .top-table td { padding: 6px 12px; border-bottom: 1px solid #eee; }
        .rank-badge { display: inline-block; width: 18px; height: 18px; line-height: 18px; text-align: center; border-radius: 50%; color: #fff; font-size: 10px; background: #999; margin-right: 5px; }
        .rank-1 { background: #d9534f; } .rank-2 { background: #fd7e14; } .rank-3 { background: #ffc107; color:#333; }
        .progress-bar-bg { background: #eee; height: 5px; width: 80px; border-radius: 3px; display: inline-block; vertical-align: middle; }
        .progress-bar-fill { background: #d9534f; height: 100%; border-radius: 3px; }
        .trend-container { text-align: center; margin-bottom: 25px; border: 1px solid #ddd; padding: 10px; border-radius: 6px; }
        .system-section { margin-bottom: 25px; page-break-inside: avoid; }
        .system-title { font-size: 14px; font-weight: bold; border-left: 4px solid #007bff; padding-left: 8px; margin-bottom: 8px; }
        .system-stats { font-size: 11px; background: #fff; padding: 6px 10px; border: 1px dashed #ccc; margin-bottom: 8px; color: #555; }
        .stat-badge { padding: 1px 5px; border-radius: 3px; color: #fff; font-size: 10px; margin: 0 2px; }
        .bg-4 { background-color: #d9534f; } .bg-3 { background-color: #fd7e14; } .bg-2 { background-color: #ffc107; color: #333; } .bg-1 { background-color: #17a2b8; }
        table.detail-table { width: 100%; border-collapse: collapse; table-layout: fixed; }
        table.detail-table th, table.detail-table td { border: 1px solid #dee2e6; padding: 5px; word-wrap: break-word; }
        table.detail-table th { background: #f8f9fa; }
    </style>
    """

    html = f"""<html><head><meta charset="UTF-8">{css}</head><body>
        <div class="report-header"><h1>运维巡检周报</h1><div class="meta">统计周期: {start_dt.strftime('%Y.%m.%d')} - {end_dt.strftime('%Y.%m.%d')}</div></div>
        <div class="global-summary-box">
            <div class="summary-top-row"><span>🚨 全局告警总数: <span style="color:#d9534f; font-size:14px;">{sum(global_levels.values())}</span> 次</span></div>
            <div style="display:flex; justify-content: space-around;">
                <div class="level-card" style="border-bottom:3px solid #d9534f;">紧急: {global_levels[4]}</div>
                <div class="level-card" style="border-bottom:3px solid #fd7e14;">严重: {global_levels[3]}</div>
                <div class="level-card" style="border-bottom:3px solid #ffc107;">中度: {global_levels[2]}</div>
                <div class="level-card" style="border-bottom:3px solid #17a2b8;">轻微: {global_levels[1]}</div>
            </div>
        </div>

        <div class="top-alerts-container">
            <div class="top-header">🏆 全局高频告警 Top {TOP_N_ALERTS}</div>
            <table class="top-table">"""
    
    max_f = top_n_data[0][1] if top_n_data else 1
    for i, (name, count) in enumerate(top_n_data):
        html += f"""<tr><td style="width:40px;"><span class="rank-badge rank-{i+1}">{i+1}</span></td>
                    <td><b>{name}</b></td>
                    <td style="text-align:right; width:140px;"><span style="color:#d9534f;">{count} 次</span>
                    <div class="progress-bar-bg"><div class="progress-bar-fill" style="width:{int(count/max_f*100)}%;"></div></div></td></tr>"""
    html += "</table></div>"

    html += f'<div class="trend-container"><div style="text-align:left; font-weight:bold; margin-bottom:5px;">📊 告警趋势图</div><img src="data:image/png;base64,{chart_img}" style="width:100%;"></div>'

    # 系统明细（带上你要的【本周统计】条）
    for cluster, data in sorted(systems_data.items(), key=lambda x: x[1]['total'], reverse=True):
        l = data['levels']
        html += f"""
        <div class="system-section">
            <div class="system-title">系统名称：{cluster}</div>
            <div class="system-stats">
                <strong>【本周统计】</strong> 告警总数: <b>{data['total']}</b> 次 
                &nbsp;&nbsp;分布: 
                <span class="stat-badge bg-4">紧急</span> {l[4]} 
                <span class="stat-badge bg-3">严重</span> {l[3]} 
                <span class="stat-badge bg-2">中度</span> {l[2]} 
                <span class="stat-badge bg-1">轻微</span> {l[1]}
            </div>
            <table class="detail-table">
                <thead><tr><th style="width:20%;">告警名称</th><th style="width:18%;">对象</th><th style="width:8%;">级别</th><th style="width:8%;">频次</th><th style="width:15%;">最近发生</th><th>摘要</th></tr></thead>
                <tbody>"""
        for r in data['rows']:
            html += f"""<tr><td>{r[2]}</td><td>{r[5]}</td>
                        <td style="text-align:center;"><span class="stat-badge bg-{r[3]}">{r[3]}</span></td>
                        <td style="text-align:center;">{r[8]}</td>
                        <td>{str(r[10])[:16]}</td><td>{r[6] if r[6] else ''}</td></tr>"""
        html += "</tbody></table></div>"

    html += "</body></html>"
    return html

if __name__ == "__main__":
    print("请输入自定义日期范围（例如 2026.2.9），直接回车则统计过去7天")
    s_in = input("开始日期: ").strip() or None
    e_in = input("结束日期: ").strip() or None
    
    rows, trend, dr = get_weekly_alerts(s_in, e_in)
    if not rows:
        print("未查询到数据。")
    else:
        html = generate_html(rows, trend, dr)
        HTML(string=html).write_pdf(REPORT_FILENAME)
        print(f"成功生成: {REPORT_FILENAME}")
