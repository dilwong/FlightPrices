# Setup built on a Fedora Linux distribution

# Install useful packages
sudo dnf install tmux python cronie
pip install jupyterlab

# Start cron service
sudo systemctl start crond.service

# Enable the cron service so that it starts automatically during system startup
sudo systemctl enable crond.service

# Set new tmux config
chmod +x change_tmux_config.sh
./change_tmux_config.sh
