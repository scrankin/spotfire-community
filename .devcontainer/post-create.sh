# link posting config to config path
ln -s $WORKSPACE_PATH/.posting/config.yaml ~/.config/posting/config.yaml

# install pre-commit
uvx pre-commit install
