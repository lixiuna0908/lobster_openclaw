import json
import sys
import glob
import os

sys.path.insert(0, '/Users/work/000code/github')
from dingtalk_stream_bridge import _send_dingtalk_text

files = glob.glob('/Users/work/000code/github/dingtalk_runtime/incoming_*.json')
files.sort(key=os.path.getmtime)

webhook = None
for fpath in reversed(files):
    with open(fpath, 'r') as f:
        data = json.load(f)
        webhook = data.get('raw', {}).get('sessionWebhook')
        if webhook:
            break

if not webhook:
    print("No webhook found.")
    sys.exit(1)

_send_dingtalk_text(webhook, "您好！GATK专用的Python环境（包含pytorch和scorevariants）已经全部安装和配置完毕，并已重新启用了CNN机器学习流程。\n您现在可以重新发送“帮我运行”来启动流程了。")
print(f"Notification sent using {webhook}")
