module.exports = {
  apps: [
    {
      name: "ctp-connection-check",
      script: "python3",
      args: "test_ctp_connection.py --env-file .env1 --connect-timeout 20 --hard-timeout-sec 60 --max-fronts 1",
      cwd: "/opt/silver-binance-main/trading/xyz_lighter_arb",
      interpreter: "none",
      autorestart: false,
      max_restarts: 0,
      time: true,
      out_file: "/tmp/ctp-connection-check.out.log",
      error_file: "/tmp/ctp-connection-check.err.log",
      merge_logs: true,
    },
  ],
};
