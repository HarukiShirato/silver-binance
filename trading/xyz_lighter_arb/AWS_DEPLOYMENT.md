# AWS 低延迟部署指南

## 1. 区域选择

根据交易所服务器位置选择 AWS 区域：

| 交易所 | 推荐区域 | 备选区域 |
|--------|----------|----------|
| Hyperliquid | ap-northeast-1 (东京) | ap-southeast-1 (新加坡) |
| Lighter | ap-northeast-1 (东京) | us-east-1 (弗吉尼亚) |

**推荐**: `ap-northeast-1` (东京) - 对两个交易所延迟都较低

## 2. EC2 实例配置

### 推荐实例类型

| 类型 | vCPU | 内存 | 网络 | 成本/月 | 适用场景 |
|------|------|------|------|---------|----------|
| c5.large | 2 | 4 GB | Up to 10 Gbps | ~$62 | 入门测试 |
| c5.xlarge | 4 | 8 GB | Up to 10 Gbps | ~$124 | **推荐生产** |
| c5n.xlarge | 4 | 10.5 GB | Up to 25 Gbps | ~$156 | 超低延迟 |

**推荐**: `c5.xlarge` - 性价比最优

### 创建实例

```bash
# 使用 AWS CLI
aws ec2 run-instances \
  --image-id ami-0ab0bbbd329f565e6 \  # Amazon Linux 2023
  --instance-type c5.xlarge \
  --key-name your-key-pair \
  --security-group-ids sg-xxx \
  --subnet-id subnet-xxx \
  --placement AvailabilityZone=ap-northeast-1a \
  --ebs-optimized \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=arbitrage-bot}]'
```

## 3. 网络优化

### 3.1 启用增强型网络

```bash
# 检查是否支持 ENA
ethtool -i eth0 | grep driver

# 应该显示: driver: ena
```

### 3.2 TCP 优化

编辑 `/etc/sysctl.conf`:

```bash
# 网络缓冲区
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
net.ipv4.tcp_rmem = 4096 1048576 16777216
net.ipv4.tcp_wmem = 4096 1048576 16777216

# TCP 快速打开
net.ipv4.tcp_fastopen = 3

# 减少 TIME_WAIT
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1

# 禁用 Nagle 算法 (应用层设置)
# TCP_NODELAY = 1
```

应用设置:
```bash
sudo sysctl -p
```

### 3.3 DNS 优化

使用 Cloudflare DNS:
```bash
echo "nameserver 1.1.1.1" | sudo tee /etc/resolv.conf
echo "nameserver 1.0.0.1" | sudo tee -a /etc/resolv.conf
```

## 4. 系统配置

### 4.1 安装依赖

```bash
# 更新系统
sudo yum update -y

# 安装 Python 3.11
sudo yum install python3.11 python3.11-pip -y

# 安装开发工具
sudo yum groupinstall "Development Tools" -y
sudo yum install python3.11-devel -y
```

### 4.2 创建运行环境

```bash
# 创建项目目录
mkdir -p /home/ec2-user/arbitrage_bot
cd /home/ec2-user/arbitrage_bot

# 上传代码
# scp -r ./arbitrage_bot/* ec2-user@your-ip:/home/ec2-user/arbitrage_bot/

# 创建虚拟环境
python3.11 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 4.3 配置环境变量

```bash
# 创建 .env 文件
cp .env.example .env

# 编辑填入实际密钥
nano .env
```

```bash
# /home/ec2-user/arbitrage_bot/.env
HL_PRIVATE_KEY=0x...
HL_WALLET_ADDRESS=0x...
LIGHTER_PRIVATE_KEY=0x...
LIGHTER_API_KEY=...
```

加载环境变量:
```bash
# 添加到 ~/.bashrc
echo 'set -a; source /home/ec2-user/arbitrage_bot/.env; set +a' >> ~/.bashrc
source ~/.bashrc
```

## 5. 进程管理

### 5.1 使用 Systemd

创建服务文件 `/etc/systemd/system/arbitrage-bot.service`:

```ini
[Unit]
Description=XYZ-Lighter Arbitrage Bot
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/arbitrage_bot
Environment=PATH=/home/ec2-user/arbitrage_bot/venv/bin
EnvironmentFile=/home/ec2-user/arbitrage_bot/.env
ExecStart=/home/ec2-user/arbitrage_bot/venv/bin/python arbitrage_bot.py
Restart=always
RestartSec=10

# 资源限制
LimitNOFILE=65535
LimitNPROC=65535

# 日志
StandardOutput=append:/home/ec2-user/arbitrage_bot/logs/stdout.log
StandardError=append:/home/ec2-user/arbitrage_bot/logs/stderr.log

[Install]
WantedBy=multi-user.target
```

启动服务:
```bash
sudo systemctl daemon-reload
sudo systemctl enable arbitrage-bot
sudo systemctl start arbitrage-bot

# 查看状态
sudo systemctl status arbitrage-bot

# 查看日志
sudo journalctl -u arbitrage-bot -f
```

### 5.2 日志轮转

创建 `/etc/logrotate.d/arbitrage-bot`:

```
/home/ec2-user/arbitrage_bot/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 ec2-user ec2-user
}
```

## 6. 监控设置

### 6.1 CloudWatch 监控

安装 CloudWatch Agent:
```bash
sudo yum install amazon-cloudwatch-agent -y
```

配置文件 `/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json`:
```json
{
  "metrics": {
    "metrics_collected": {
      "cpu": {
        "measurement": ["cpu_usage_active"],
        "metrics_collection_interval": 60
      },
      "mem": {
        "measurement": ["mem_used_percent"],
        "metrics_collection_interval": 60
      },
      "net": {
        "measurement": ["net_bytes_recv", "net_bytes_sent"],
        "metrics_collection_interval": 60
      }
    }
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/home/ec2-user/arbitrage_bot/logs/arbitrage.log",
            "log_group_name": "arbitrage-bot",
            "log_stream_name": "{instance_id}"
          }
        ]
      }
    }
  }
}
```

启动:
```bash
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s
```

### 6.2 设置告警

```bash
# CPU 使用率超过 80%
aws cloudwatch put-metric-alarm \
  --alarm-name "arbitrage-bot-cpu-high" \
  --metric-name CPUUtilization \
  --namespace AWS/EC2 \
  --statistic Average \
  --period 300 \
  --threshold 80 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=InstanceId,Value=i-xxx \
  --evaluation-periods 2 \
  --alarm-actions arn:aws:sns:ap-northeast-1:xxx:alerts
```

## 7. 安全配置

### 7.1 安全组规则

```bash
# 入站规则
- SSH (22): 仅你的 IP
- 无其他入站

# 出站规则
- HTTPS (443): 0.0.0.0/0 (API 访问)
- WSS (443): 0.0.0.0/0 (WebSocket)
```

### 7.2 IAM 角色

创建最小权限角色:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:arbitrage-bot:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
```

### 7.3 密钥管理

使用 AWS Secrets Manager:
```bash
# 存储密钥
aws secretsmanager create-secret \
  --name arbitrage-bot/api-keys \
  --secret-string '{"HL_PRIVATE_KEY":"xxx","LIGHTER_API_KEY":"xxx"}'

# 在代码中获取
import boto3
client = boto3.client('secretsmanager')
response = client.get_secret_value(SecretId='arbitrage-bot/api-keys')
```

## 8. 延迟测试

### 8.1 测量脚本

```python
# latency_test.py
import asyncio
import time
import aiohttp

async def test_latency(url, count=10):
    latencies = []
    async with aiohttp.ClientSession() as session:
        for _ in range(count):
            start = time.perf_counter()
            async with session.get(url) as resp:
                await resp.read()
            latency = (time.perf_counter() - start) * 1000
            latencies.append(latency)

    avg = sum(latencies) / len(latencies)
    print(f"{url}: avg={avg:.1f}ms, min={min(latencies):.1f}ms, max={max(latencies):.1f}ms")

asyncio.run(test_latency("https://api.hyperliquid.xyz/info"))
asyncio.run(test_latency("https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"))
```

### 8.2 预期延迟

| 位置 | Hyperliquid | Lighter | 总往返 |
|------|-------------|---------|--------|
| 东京 EC2 | 5-15ms | 10-20ms | 15-35ms |
| 新加坡 EC2 | 20-40ms | 15-30ms | 35-70ms |
| 本地 (中国) | 100-200ms | 150-250ms | 250-450ms |

**目标**: 总延迟 < 50ms

## 9. 快速部署脚本

一键部署脚本 `deploy.sh`:

```bash
#!/bin/bash
set -e

# 配置
INSTANCE_TYPE="c5.xlarge"
REGION="ap-northeast-1"
KEY_NAME="your-key-pair"

echo "=== 创建 EC2 实例 ==="
INSTANCE_ID=$(aws ec2 run-instances \
  --region $REGION \
  --image-id ami-0ab0bbbd329f565e6 \
  --instance-type $INSTANCE_TYPE \
  --key-name $KEY_NAME \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "Instance ID: $INSTANCE_ID"

echo "=== 等待实例运行 ==="
aws ec2 wait instance-running --instance-ids $INSTANCE_ID --region $REGION

PUBLIC_IP=$(aws ec2 describe-instances \
  --instance-ids $INSTANCE_ID \
  --region $REGION \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text)

echo "Public IP: $PUBLIC_IP"

echo "=== 上传代码 ==="
scp -r ./arbitrage_bot ec2-user@$PUBLIC_IP:/home/ec2-user/

echo "=== 初始化环境 ==="
ssh ec2-user@$PUBLIC_IP << 'EOF'
cd /home/ec2-user/arbitrage_bot
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
mkdir -p logs data
EOF

echo "=== 部署完成 ==="
echo "SSH: ssh ec2-user@$PUBLIC_IP"
echo "配置 .env 后启动: python arbitrage_bot.py"
```

## 10. 常见问题

### Q1: WebSocket 频繁断开
- 检查 `net.ipv4.tcp_keepalive_time` 设置
- 启用应用层心跳 (ping/pong)

### Q2: 延迟波动大
- 使用 Placement Group
- 检查是否有 CPU throttling
- 考虑使用专用主机

### Q3: 内存不足
- 检查 Python 进程内存泄漏
- 增加 swap 或升级实例

### Q4: API 限流
- 实现请求队列
- 使用指数退避重试
- 考虑 WebSocket 代替轮询
