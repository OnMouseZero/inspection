import sqlite3
import datetime
import logging
import os
from flask import Flask, request, jsonify

# --- 配置 ---
DB_FILE = 'alerts.db'
PORT = 5001

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

app = Flask(__name__)

def init_db():
    """
    初始化数据库
    重新规划了字段，与 DingTalk 模板对齐，确保核心信息完整
    """
    # 建议：如果字段变动较大，手动删除旧 db 文件或做迁移。这里为了演示简单，建议您先删除旧 alerts.db
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # 核心字段解释：
        # alert_name  : 对应 metricName (如 Node内存使用率)
        # cluster     : 对应 clusterName (如 生产-Cluster)
        # namespace   : 对应 namespace (如 monitoring)
        # level       : 对应 alertLevel (如 1, 2, 3, 4)
        # metric_type : 对应 metricType (如 资源, 业务)
        # target      : 对应 alertTarget (如 192.168.1.10)
        # key_info    : 对应 alertPoint (简短的关键信息，用于 PDF 表格展示，避免截断)
        # detail_info : 对应 alertContent (详细规则描述，用于 PDF 详情部分)
        
        c.execute('''CREATE TABLE IF NOT EXISTS weekly_alerts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        alert_name TEXT,
                        cluster TEXT,
                        namespace TEXT,
                        level TEXT,
                        metric_type TEXT,
                        target TEXT,
                        key_info TEXT,
                        detail_info TEXT,
                        starts_at TEXT,
                        created_at TEXT
                    )''')
        conn.commit()
        conn.close()
        logger.info(f"✅ 数据库 {DB_FILE} 初始化完成 (Schema 已升级)")
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败: {e}")

@app.route('/health', methods=['GET'])
def health_check():
    return "I am alive!", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """接收 Alertmanager 发来的 JSON 并映射到新字段"""
    try:
        data = request.json
        if not data:
            return "No JSON data received", 400

        alerts = data.get('alerts', [])
        stored_count = 0
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        for alert in alerts:
            # 只记录触发状态的告警，忽略恢复信息
            if alert.get('status') == 'firing':
                labels = alert.get('labels', {})
                annotations = alert.get('annotations', {})

                # 1. 提取基础信息 (优先使用自定义标签，没有则回退到标准标签)
                alert_name = labels.get('metricName', labels.get('alertname', 'Unknown Alert'))
                cluster = labels.get('clusterName', labels.get('cluster', 'default'))
                namespace = labels.get('namespace', '-')
                
                # 2. 提取等级和类型
                level = labels.get('alertLevel', '0') # 默认为 0
                metric_type = labels.get('metricType', '通用')
                
                # 3. 提取对象 (优先取 alertTarget)
                target = labels.get('alertTarget', labels.get('instance', 'Unknown'))

                # 4. 核心：处理摘要和详情
                # key_info 用于表格显示，取 alertPoint (通常比较短)
                # 如果没有 alertPoint，尝试截取 description 的前 50 个字符
                raw_desc = annotations.get('description', '')
                key_info = annotations.get('alertPoint', raw_desc[:50] + '...' if len(raw_desc)>50 else raw_desc)
                
                # detail_info 用于详情展示，取 alertContent
                detail_info = annotations.get('alertContent', annotations.get('summary', raw_desc))

                # 5. 时间处理
                starts_at = alert.get('startsAt', '')
                # 尝试将 UTC 时间转换为更友好的本地时间字符串 (可选)
                try:
                    dt = datetime.datetime.strptime(starts_at.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                    # 假设是 UTC，简单 +8 处理展示 (仅做字符串转换，建议按需调整)
                    dt = dt + datetime.timedelta(hours=8)
                    starts_at = dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass # 保持原样

                now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # 插入新结构的表
                c.execute('''INSERT INTO weekly_alerts
                             (alert_name, cluster, namespace, level, metric_type, target, key_info, detail_info, starts_at, created_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (alert_name, cluster, namespace, level, metric_type, target, key_info, detail_info, starts_at, now_str))
                
                stored_count += 1
                logger.info(f"📥 存入: [{cluster}] {alert_name} (Lv.{level})")

        conn.commit()
        conn.close()
        return jsonify({"status": "success", "stored": stored_count}), 200

    except Exception as e:
        logger.error(f"❌ 出错: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # 建议先手动删除旧的 alerts.db 文件，否则可能会报错 table 已存在但列不对
    if os.path.exists(DB_FILE):
        # 简单检查一下是否需要重新初始化（可选逻辑，这里简单起见建议手动处理）
        pass
        
    init_db()
    logger.info(f"🚀 告警接收服务已启动，监听端口: {PORT}...")
    app.run(host='0.0.0.0', port=PORT)
