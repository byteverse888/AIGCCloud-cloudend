#!/usr/bin/env python3
"""
激励结算模块入口
通过 cron 定时调用，支持以下子命令：

  python -m incentive.main collect     # 每小时统计（cron 每小时调用）
  python -m incentive.main settle      # 每日清算（cron 每天凌晨 2 点调用）
  python -m incentive.main node <eth>  # 查询节点信息
  python -m incentive.main network     # 查询全网信息
  python -m incentive.main history <eth> [--limit 50]  # 查询节点历史

cron 配置示例：
  0 * * * *  cd /path/to/aigccloud-cloudend && python -m incentive.main collect
  0 2 * * *  cd /path/to/aigccloud-cloudend && python -m incentive.main settle
"""
import argparse
import sys
import traceback

from incentive.logger import logger


def main():
    parser = argparse.ArgumentParser(
        description="算力贡献积分激励结算系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", help="子命令")

    # collect
    sub.add_parser("collect", help="每小时节点统计（从 K8s 采集数据，计算在线积分）")

    # settle
    sub.add_parser("settle", help="每日积分清算（批量转账到联盟链）")

    # node
    p_node = sub.add_parser("node", help="查询节点信息")
    p_node.add_argument("eth_address", help="节点 ETH 地址")

    # network
    sub.add_parser("network", help="查询全网信息")

    # history
    p_hist = sub.add_parser("history", help="查询节点积分历史")
    p_hist.add_argument("eth_address", help="节点 ETH 地址")
    p_hist.add_argument("--limit", type=int, default=50, help="返回记录数（默认 50）")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == "collect":
            from incentive.collector import run_collect
            run_collect()

        elif args.command == "settle":
            from incentive.settlement import run_settlement
            run_settlement()

        elif args.command == "node":
            from incentive.query import print_node_info
            print_node_info(args.eth_address)

        elif args.command == "network":
            from incentive.query import print_network_info
            print_network_info()

        elif args.command == "history":
            from incentive.query import query_node_history
            records = query_node_history(args.eth_address, limit=args.limit)
            if not records:
                print(f"未找到节点 {args.eth_address} 的历史记录")
                return
            print(f"\n节点 {args.eth_address} 最近 {len(records)} 条记录:")
            print(f"{'─'*80}")
            for r in records:
                print(
                    f"  [{r['createdAt'][:19]}] "
                    f"{r['type']:15s} | "
                    f"积分: {r['amount']:>10.2f} | "
                    f"{r['description'][:60]}"
                )
            print(f"{'─'*80}\n")

    except Exception as e:
        logger.error(f"[Main] 执行失败: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
