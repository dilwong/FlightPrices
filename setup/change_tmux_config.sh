#!/bin/bash

# Define the new key prefix
echo "set-option -g prefix C-a" > ~/.tmux.conf

# Unbind the "Ctrl + b" key from the default prefix command
echo "unbind-key C-b" >> ~/.tmux.conf

# Set the new key prefix to "Ctrl + a" to send the prefix command to `tmux`
echo "bind-key C-a send-prefix" >> ~/.tmux.conf

# Set the new horizontal split key
echo "bind-key v split-window -h" >> ~/.tmux.conf

# Set the new vertical split key
echo "bind-key h split-window -v" >> ~/.tmux.conf

# Unbind the "%" (percentage) key from the vertical division command
echo "unbind-key %" >> ~/.tmux.conf

# Unbind the " (double quote) character from the horizontal division command
echo 'unbind-key "\""' >> ~/.tmux.conf

# Load the new `tmux` settings
tmux source-file ~/.tmux.conf
