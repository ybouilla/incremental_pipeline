
Since i think systemd is not present on wsl windows
```shell
sudo service postgresql start
sudo -u postgres psql
# once in posgresql shell enter:
create database licenses_db;
create user myuser with password 'my_password';
\c licenses_db  
# above : check if connection is successful
```