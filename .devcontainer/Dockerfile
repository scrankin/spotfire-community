FROM python:3.13-bookworm

# install zsh
RUN apt-get update && \
    apt-get install -y zsh curl git

# install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
	&& install -m 755 /root/.local/bin/uv /usr/local/bin/uv \
    && install -m 755 /root/.local/bin/uvx /usr/local/bin/uvx \
	&& rm -rf /root/.local/bin/uv \
	&& rm -rf /root/.local/bin/uvx

# create user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME
ENV HOME=/home/$USERNAME
ENV SHELL=/bin/zsh

# set zsh and add default .zshrc
RUN chsh -s $(which zsh) $USERNAME
ADD .zshrc $HOME/.zshrc
RUN chown $USERNAME:$USERNAME $HOME/.zshrc

USER $USERNAME

# install rustup (stable toolchain)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --no-modify-path
ENV PATH="/home/$USERNAME/.cargo/bin:$PATH"

# install oh-my-zsh
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended


# install posting
RUN uv tool install --python 3.13 posting
RUN mkdir -p /home/$USERNAME/.config/posting

RUN uv tool update-shell

CMD ["zsh"]
