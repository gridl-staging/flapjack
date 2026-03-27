# systemd service example

Use these templates for production-style systemd deployments of Flapjack.

## Quick setup

1. Copy the service unit:

   ```bash
   sudo cp engine/examples/systemd/flapjack.service /etc/systemd/system/flapjack.service
   ```

2. Create the service account and writable directories used by the default `/opt/flapjack/...` layout:

   ```bash
   id flapjack >/dev/null 2>&1 || sudo useradd -r -s /sbin/nologin flapjack
   sudo mkdir -p /opt/flapjack/bin /opt/flapjack/data
   sudo chown -R flapjack:flapjack /opt/flapjack
   ```

3. Install a known-good Linux `flapjack` binary at `/opt/flapjack/bin/flapjack` before enabling the service. The executable must be built for the target host architecture (Linux ELF for this unit template):

   ```bash
   sudo install -m 0755 /path/to/linux/flapjack /opt/flapjack/bin/flapjack
   file /opt/flapjack/bin/flapjack
   ```

4. Create an env file from the template. Then uncomment `EnvironmentFile=/etc/flapjack/env` in `/etc/systemd/system/flapjack.service` and set host-specific values. For production, keep `FLAPJACK_ENV=production` and replace `FLAPJACK_ADMIN_KEY` with a strong value that is at least 16 characters:

   ```bash
   sudo mkdir -p /etc/flapjack
   sudo cp engine/examples/systemd/env.example /etc/flapjack/env
   sudoedit /etc/flapjack/env
   ```

5. Reload systemd and enable/start the service:

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now flapjack
   ```

6. Verify the public health and readiness probes:

   ```bash
   sudo systemctl status flapjack
   curl -f http://127.0.0.1:7700/health
   curl -f http://127.0.0.1:7700/health/ready
   ```

## Notes

- Default unit runs as the dedicated `flapjack` user, expects `/opt/flapjack` to be owned by that account, and uses `/opt/flapjack/bin/flapjack` as `ExecStart` (ops-managed path).
- If you installed with `install.sh`, switch `User=` to that account, replace `~/.flapjack/bin/flapjack` with the matching `/home/<user>/.flapjack/bin/flapjack` path shown in the unit comments, and adjust the home-directory hardening notes there.
- For full env variable definitions/defaults, see [OPS_CONFIGURATION.md](../../docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md).
- For broader deployment runbooks, see [DEPLOYMENT.md](../../docs2/3_IMPLEMENTATION/DEPLOYMENT.md).
