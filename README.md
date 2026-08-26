# VM setup

How to set up the Prefect and Postgres stack using Nectar Cloud

## Virtual Machine Setup

### Key Setup

Before doing anything, we need to create a new SSH key pair. We can use ssh-agent:

```bash
ssh-keygen -f ~/.ssh/prefect -t ed25519
```

We need to add the public key as a key pair to Nectar. We can do this from _Compute_ > _Key Pairs_ and select _Import Public Key_

- Choose an appropriate name such as "prefect"
- Key Type is "SSH Key"
- Copy and paste the **Public Key** made with ssh-keygen

> IMPORTANT
> If the all private SSH keys used to access the VM are lost, there is no realistic way to regain SSH access. Console access is possible, but adding a new key this way must be done by hand, which is finicky and not recommended. This document exists partially to serve as a disaster recovery tool for this scenario.

### Creation

From _Compute_ > _Instances_ select _Launch Instance_

**Details**

- Instance Name
  - prefect_vm
- Availability Zone
  - Pawsey
- Count
  - 1

**Source**

- Boot Source
  - Image
- Create New Volume
  - Yes
- Volume Size
  - 50 GB should be fine
  - Do not delete volume on instance delete
- Choose the image "Ubuntu 26.04 amd64"

**Flavour**

- Choose m3.small

**Networks**

- Leave untouched

**Security Groups**

- If HTTP/HTTPS is available, select it
- If SSH is available, select it
- **If either of these are unavailable, they must be created. We can do this after VM creation.**

**Key Pair**

- Add The key pair created in the previous section

**Configuration**

**Customization Script**

- Copy and paste the `cloud-init.yaml` file
- Disk Partition
  - Automatic

**Server Groups**

- Leave untouched

**Metadata**

- Leave untouched

### Security Groups

If HTTP/HTTPS or SSH were available in the VM creation process, this can be skipped.

This step is to set up both SSH and HTTPS access to the server.

Go to _Network_ > _Security Groups_ and select _Create Security Group_

- Name it HTTP/HTTPS

Add a new rule

- Rule
  - Custom TCP Rule
- Description
  - HTTP
- Direction
  - Ingress
- Port
  - 80
- Remote
  - CIDR
- CIDR
  - 0.0.0.0/0

Add a new rule

- Rule
  - Custom TCP Rule
- Description
  - HTTPS
- Direction
  - Ingress
- Port
  - 443
- Remote
  - CIDR
- CIDR
  - 0.0.0.0/0

Go back to the _Security Groups_ page and once again select _Create Security Group_

- Name it SSH

Add a new rule

- Rule
  - Custom TCP Rule
- Description
  - SSH
- Direction
  - Ingress
- Port
  - 22
- Remote
  - CIDR
- CIDR
  - 0.0.0.0/0

Security groups can be added to a VM instance by navigating to the _Compute_ > _Instances_ page and selecting the dropdown, then selecting _Edit Instance_.

### Test SSH

Once the VM has started and the previous steps have been completed, we can test that the SSH connection works by using the VM's public IP address from the _Compute_ > _Instances_ page.

```bash
ssh -i ~/.ssh/prefect ubuntu@
```

### Password Setup

We have an SSH key for external access, but if we want to use the Console for access, we need to set up a password for the ubuntu user.

SSH into the VM and execute the following command to set the password:

```bash
sudo passwd ubuntu
```

### Adding Another Key to the VM

Each key pair (private & public) should be used by a single user. It is good practice to make a new key for each required user.

This first step should be completed by the user that is being granted access. At no point should a private key be shared between users.

Create a new key pair with ssh-agent:

````bash

```bash
ssh-keygen -f ~/.ssh/{KEY_NAME} -t ed25519
````

The user can then send the **public key** to the user that already has access.

The user with access can copy the public key contents to their clipboard and SSH into the VM.

Open `~/.ssh/authorized_keys` with

```bash
vim ~/.ssh/authorized_keys
```

Then paste the public key into a new line with Ctrl+Shift+V

The new user can test the new connection with

```bash
ssh -i ~/.ssh/{KEY_NAME} ubuntu@{VM_IP}
```

## Database Setup

### Creation

In _Database_ > _Instances_ then select _Launch Instance_

**Details**

- Volume Size
  Around 30 GB should be plenty for us
- Datastore
  - Postgres
- Flavour
  - Small is fine

**Networking**

- Add the appropriate network. From what I can tell, you can only choose one, otherwise an error will prevent the database creation

**Database Access (Important!)**

- Allowed CIDRs
  - This is a security consideration. We only want the Prefect server itself to be able to talk to the database, so we add the VM's public IP address followed by _\/32_

**Initialize Databases**

- Initial Databases
  - prefect
- Initial Admin User
  - prefect
- Password
  - prefect

### Post-creation Setup

Before spinning up the server, we still need to configure a few things.

First, add the database hostname to the `.env` file, named PREFECT_API_DATABASE_CONNECTION_HOST

We also need to give the appropriate access and ownership to the Prefect user. For this, we need a root password.
From the database instances panel, under the **Actions** column, choose _Manage Root Access_ from the dropdown menu.
Select _Enable Root_. This will generate a root password - make note of it.
Connect to the VM, as it's the only way to access the database. Then use the following command to connect to the database:

```bash
psql postgresql://postgres:{ROOT_PASSWORD}@{HOST_NAME}:5432/prefect
```

And execute the following commands:

```sql
GRANT ALL ON SCHEMA public TO prefect;
ALTER SCHEMA public OWNER TO prefect;
exit
```

Then, to ensure the root password is not saved to the bash history

```bash
cat /dev/null > ~/.bash_history && history -c && exit
```

## DNS

We would like to set a domain name for our server. This is not only a cosmetic change, Caddy requires a registered domain in order to sign the certificates that allow HTTPS connectivity.

Go to _DNS_ > _Zones_ and find the project's zone. Select _Create Record Set_

- Type
  - A - Address Record
- Name
  - prefect.{ZONE_NAME}.
- TTL
  - 3600
- Record
  - {VM_IP_ADDRESS}

Once in place, we can test that this worked by attempting to connect to the VM via SSH:

```bash
ssh -i {PRIVATE_KEY} ubuntu@prefect.{ZONE_NAME}
```

## Prefect Server Setup

With everything else set up, the environment is ready for the Prefect server itself.

SSH into the VM and copy the following files from this repository to it:

- Caddyfile -> /home/ubuntu/containers/Caddyfile
- docker-compose.yaml -> /home/ubuntu/docker-compose.yaml
- example.env -> /home/ubuntu/.env

The .env file needs to be altered to contain the appropriate configuration.

- PUBLIC_HOST: This is the full hostname of the VM. It's what we set as the A-name in the DNS section
- PREFECT_PASSWORD: This is a password that will bar access to Prefect. Use a password generator to create one.
- PREFECT_API_DATABASE_CONNECTION_HOST: This is the database hostname. It can be found in _Database_ > _Instances_ under the _Host_ heading.

Once all of this is set, run

```bash
docker-compose up -d
```

and make sure everything starts properly.
Check the logs, particularly of the Prefect server:

```bash
docker logs prefect
```

The server can be accessed via the web UI by navigating to `https://prefect.{ZONE_NAME}`
The server will prompt you for a password, which should be supplied in the format `admin:password` where the password was set in the `.env` file.
