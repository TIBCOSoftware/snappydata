<a id="ssh"></a>
## Configuring SSH Login without Password

!!! Note
	Before you begin ensure that you have configured SSH login without password.

By default, Secure Socket Shell (SSH) requires a password for authentication on a remote server.
This setting needs to be modified to allow you to login to the remote host through the SSH protocol, without having to enter your SSH password multiple times when working with SnappyData.

To install and configure SSH, do the following:

1. **Install SSH** <br>
	To install SSH,  type: </br>`sudo apt-get install ssh` </br>
    Mac OS X has a built-in SSH client.

2. **Generate an RSA key pair**<br>
    To generate an RSA key pair run the following command on the client computer, </br>
    `ssh-keygen -t rsa` </br>
    Press **Enter** when prompted to enter the file in which to save the key, and for the pass phrase.

3.  **Copy the Public Key**<br>
    Once the key pair is generated, copy the contents of the public key file, to the authorized key on the remote site, by typing `cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys`
