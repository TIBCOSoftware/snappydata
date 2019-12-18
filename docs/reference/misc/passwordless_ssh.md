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

    Mac OS X has a built-in SSH client.

3. **Generate an RSA key pair**<br>
    Generate an RSA key pair by running the following command on your system.
        `ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ''`
    This will create two files (a key pair) at `~/.ssh/` path: 1) `id_rsa` which is the private key and 2) `id_rsa.pub` - the public key.

4.  **Copy the Public Key**<br>
    Once the key pair is generated, append the contents of this public key file, to the authorized key file (typically `~/.ssh/authorized_keys`) on the remote host(s).
    With this, you can ssh to this remote host(s) from your local system, without providing the password.

    For the single node setup, you can simply append it by executing `cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys` on your system.

5. **Two-Way Access**<br>
    Optionally, if you want to also do ssh login from remote host to your system without providing the password, copy your `id_rsa` file generated above and place it at `~/.ssh/` on the remote host(s).
    Make sure you do not have a `id_rsa` file already created at that location.
        `scp ~/.ssh/id_rsa <remote-host>:~/.ssh/`    # You'll be asked for password here.

    Also, make sure it is not writable for other users.
        `chmod 600 ~/.ssh/id_rsa`    # On remote host
