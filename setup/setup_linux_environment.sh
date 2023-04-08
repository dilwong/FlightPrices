# Setup built on a Fedora Linux distribution

sudo dnf install tmux python
pip install jupyterlab

# Set new tmux config
chmod +x change_tmux_config.sh
./change_tmux_config.sh
