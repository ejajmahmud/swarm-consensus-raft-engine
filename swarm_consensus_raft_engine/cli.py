"""CLI for swarm-consensus-raft-engine."""
import sys
import argparse
from .engine import RaftCluster

def main():
    parser = argparse.ArgumentParser(description="Swarm Consensus Raft Engine CLI")
    parser.add_argument("--demo", action="store_true", help="Run multi-agent leader election & log replication demo")
    args = parser.parse_args()

    if args.demo:
        nodes = ["agent-alpha", "agent-beta", "agent-gamma"]
        cluster = RaftCluster(nodes)
        leader = cluster.elect_leader("agent-alpha")
        print(f"[+] Cluster initialized: {nodes}")
        print(f"[+] Leader successfully elected: {leader}")
        success = cluster.broadcast_command(leader, "EXECUTE_TOOL", {"tool": "search_db", "query": "latest_metrics"})
        print(f"[+] Command replicated across cluster: {success}")
        for nid, node in cluster.nodes.items():
            print(f"    Node {nid} (role={node.role.value}): applied {len(node.state_machine)} commands")
    else:
        parser.print_help()
    sys.exit(0)

if __name__ == "__main__":
    main()
