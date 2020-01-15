<a id="ssh"></a>
## Configuring SSH Login without Password

By default, Secure Socket Shell (SSH) requires a password for authentication on a remote server.
But with some changes in the configuration, you can login to the remote host through the SSH protocol, without having to enter your SSH password multiple times.

This is specially helpful when using the cluster start/stop scripts like `snappy-start-all.sh` to launch the SnappyData cluster spanning multiple hosts.

These steps are provided as a guide for setting up passwordless SSH. Check with your system administrator for more details.

1. **Check SSH** <br>
    Check if ssh is installed on your Linux-based host(s) using below command.
        `systemctl status sshd`

    Or on systems where `systemctl` is not available (for example, some versions of Linux Mint), use below command:
        `service ssh status`

2. **Install and start SSH** <br>
	To install SSH on Ubuntu systems, run `apt update && apt install openssh-server`
    On RHEL/CentOS systems, the command is `yum -y install openssh-server openssh-clients`

    Then enable and start the SSH service:
        `systemctl enable sshd` Or `systemctl enable ssh`
        `systemctl start sshd` Or `systemctl start ssh`

    Perform above two steps for all the systems which will be part of the SnappyData cluster.

    Mac OS X has a built-in SSH client.

3. **Generate an RSA key pair**<br>
    Generate an RSA key pair on your local or primary system by running the following command.
        `ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ''`
    This will create two files (a key pair) at `~/.ssh/` path: 1) `id_rsa` which is the private key and 2) `id_rsa.pub` - the public key.

4.  **Copy the Public Key**<br>
    Once the key pair is generated, append the contents of the public key file `id_rsa.pub`, to the authorized key file `~/.ssh/authorized_keys` on all the remote hosts.

    With this, you can ssh to these remote hosts from your local system, without providing the password.
    This also enables you to execute cluster start, stop or status scripts from your local system.

    For the single node setup, you can simply append it by executing `cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys` on your system.

5. **Two-Way Access**<br>
    Optionally, if you want to also do ssh login from remote hosts to your system without providing the password, copy your `id_rsa` file generated above and place it at `~/.ssh/` on the remote hosts.
    Make sure you do not already have a `id_rsa` file present at that location on remote hosts.
        `scp ~/.ssh/id_rsa <remote-host>:~/.ssh/`    # You'll be asked for password here.

    Also, make sure it is not writable for other users.
        `chmod 600 ~/.ssh/id_rsa`    # On remote host
