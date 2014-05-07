#!/usr/bin/env bash

set -e

#check to make sure this is a 64-bit machine
getconf LONG_BIT | grep 64 > /dev/null  || ( echo "Git Fusion requires a 64-bit machine. Exiting." && exit 1)

echo ''
echo 'This script will install the requirements for Git Fusion. It will:'
echo '  * Download, build, and install Git 1.8.2.3 and its build dependencies'
echo '  * Download, build, and install Python 3.3.2, P4Python, the P4API, and the p4 command line'
echo '  * Download, build, and install libgit2 and pygit2'
echo '  * Download, build, and install pytz'
echo '  * Install the Git Fusion scripts'
echo '  * Optionally, create a user account for Git'
echo ''
echo 'To install Git Fusion you will need to have Perforce super user access and you will need'
echo 'to be able to install triggers on the Perforce server machine itself. This script will'
echo 'only install software to this machine. We will detail the trigger installation process'
echo 'after Git Fusion is configured locally.'
echo ''
echo "Do you wish to continue?"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) break;;
        No ) exit;;
    esac
done

echo 'Grabbing build tools...'
sudo apt-get update
sudo apt-get install -y build-essential

echo 'Grabbing the required version of Git and its build dependencies...'
wget http://git-core.googlecode.com/files/git-1.8.2.3.tar.gz
sudo apt-get -y install tk8.5 tcl8.5
sudo apt-get -y install autoconf
sudo apt-get -y install gettext
sudo apt-get -y install zlib1g-dev
sudo apt-get -y install libcurl4-openssl-dev

echo Building Git...
tar xvzf git-1.8.2.3.tar.gz
cd git-1.8.2.3
autoconf
./configure
make
make test || :
sudo make install
cd ..


echo 'Grabbing the required versions of Python, P4Python, and the P4API...'
sudo apt-get -y build-dep python3.2
sudo apt-get -y install libreadline-dev libncurses5-dev libssl1.0.0 tk8.5-dev liblzma-dev
sudo apt-get -y install openssl
wget http://python.org/ftp/python/3.3.2/Python-3.3.2.tgz
tar xfz Python-3.3.2.tgz
cd Python-3.3.2
./configure
make
sudo make install
cd ..

wget http://ftp.perforce.com/perforce/r14.1/bin.tools/p4python.tgz
wget http://ftp.perforce.com/perforce/r14.1/bin.linux26x86_64/p4api.tgz
wget http://ftp.perforce.com/perforce/r14.1/bin.linux26x86_64/p4
wget http://ftp.perforce.com/perforce/r14.1/bin.linux26x86_64/p4d

echo 'Building P4Python...'
tar xvzf p4python.tgz 
tar xvzf p4api.tgz
chmod +x p4 p4d
sudo cp p4 /usr/local/bin/p4
mv p4 p4python-*
mv p4d p4python-*
cd p4python-*
export PATH=.:$PATH
python3 setup.py build --apidir ../p4api-2014.1.*/ --ssl
python3 p4test.py
sudo python3 setup.py install --apidir ../p4api-2014.1.*/ --ssl

echo 'Testing the installation...'
echo import P4 > p4python_version_check.py
echo 'print(P4.P4.identify())' >> p4python_version_check.py
python3 p4python_version_check.py
cd ..

echo "Grabbing libgit2, pygit, and their dependencies..."
sudo apt-get -y install cmake
git clone https://github.com/libgit2/libgit2.git
mkdir -p libgit2/build
cd libgit2
git checkout v0.18.0 
cd build
cmake ..
cmake --build .
sudo cmake --build . --target install
cd ../..

git clone https://github.com/libgit2/pygit2.git
cd pygit2
git checkout v0.18.0 
export LIBGIT2="/usr/local"
export LDFLAGS="-Wl,-rpath='$LIBGIT2/lib',--enable-new-dtags $LDFLAGS"
python3 setup.py build
sudo python3 setup.py install
python3 setup.py test
cd ..

echo 'Installing pytz for time zone support...'
wget --no-check-certificate https://pypi.python.org/packages/source/p/pytz/pytz-2013b.tar.bz2
tar jxf pytz-2013b.tar.bz2
cd pytz-2013b
python3 setup.py build
sudo python3 setup.py install
cd ..

echo ""
read -e -p "What directory should the Git Fusion scripts be installed to? " -i "/usr/local/git-fusion/bin" FILEPATH

sudo mkdir -p $FILEPATH
sudo cp *.py $FILEPATH
sudo cp *.txt $FILEPATH
sudo cp Version $FILEPATH
# may need to 
echo "Git Fusion installed to $FILEPATH"
echo ""

echo "Do you wish to create a user account for Git? This will be the account your users use when connecting to Git."
echo "For example: git clone <user account name>@$HOSTNAME:repo"
echo "If you choose not to create a user for Git, your current username will be used."
select yn in "Yes" "No"; do
    case $yn in
        Yes ) read -e -p "Account name? " -i "git" ACCOUNTNAME
              echo "Creating git user account $ACCOUNTNAME..."; 
              sudo adduser --gecos "" $ACCOUNTNAME;
              break;;
        No ) ACCOUNTNAME="$USER"; break;;
    esac
done

echo ''
echo 'Enabling logging to the system log...'
sudo cp git-fusion.log.conf /etc/git-fusion.log.conf
echo ':syslogtag,contains,"git-fusion[" -/var/log/git-fusion.log' > /tmp/out
echo ':syslogtag,contains,"git-fusion-auth[" -/var/log/git-fusion-auth.log' >> /tmp/out
sudo cp /tmp/out /etc/rsyslog.d/git-fusion.conf
echo "/var/log/git-fusion-auth.log" | cat -  /etc/logrotate.d/rsyslog  > /tmp/out && sudo cp /tmp/out /etc/logrotate.d/syslog
echo "/var/log/git-fusion.log" | cat -  /etc/logrotate.d/rsyslog  > /tmp/out && sudo cp /tmp/out /etc/logrotate.d/syslog
sudo service rsyslog restart

echo ""
echo "===================================================================================="
echo 'Automated install complete! Now a few final bits to do manually.'
echo "===================================================================================="
echo ''
echo 'Create a working directory for the Git Fusion instance in your filesystem.'
echo "Suggested location is /home/$ACCOUNTNAME/.git-fusion"
echo ''
echo 'Create p4gf_environment.cfg configuration file with the following contents:'
echo ''
echo '[environment]'
echo 'P4GF_HOME=</path/to/working/directory/for/git-fusion>'
echo 'P4PORT=<your Perforce port>'
echo ''
echo "Add the following export lines to the top of the $ACCOUNTNAME .bashrc (/home/$ACCOUNTNAME/.bashrc)"
echo ''
echo "export PATH=$FILEPATH"':$PATH'
echo 'export P4GF_ENV=</path/to/p4gf_environment.cfg>'
echo ''
echo 'After updating your .bashrc file run:'
echo ''
echo "source /home/$ACCOUNTNAME/.bashrc"
echo 'p4 -u <Perforce super user account> login'
echo 'p4gf_super_init.py --user <Perforce super user account>'
echo ''
echo 'Make sure to set a password for git-fusion-user and run p4 login as git-fusion-user to setup a ticket'
echo ''
echo 'Git Fusion requires a trigger to be installed on your Perforce server to '
echo 'properly support atomic checkins in Git. To install the trigger:'
echo ''
echo '1) Copy "p4gf_submit_trigger.py" to your Perforce server machine'
echo '2) Run the following to generate the trigger lines needed by Git Fusion'
echo '   python p4gf_submit_trigger.py --generate-trigger-entries "/absolute/path/to/python" "/absolute/path/to/p4gf_submit_trigger.py"'
echo '3) As a Perforce super user run "p4 triggers" and add those entries.'
echo ''
echo 'You will need to add triggers as above for each depot where you want to enable Git Fusion.'
echo 'The final step is to setup the version counter by running the following commands from the Perforce server'
echo ''
echo 'p4 -u git-fusion-user login'
echo 'python p4gf_submit_trigger.py --set-version-counter <your server port>'
echo ''
echo 'If your server runs in Unicode mode, you will need to make a slight change to the trigger script:'
echo 'For unicode servers uncomment the following line'
echo "#CHARSET = ['-C', 'utf8']"
echo ''
echo 'You will need to add a cronjob to check for and install new SSH keys.'
echo 'See the Git Fusion Guide section on "Create a cron job" for details.'
echo ''
echo 'Now either add user keys and/or run the following to create a repository'
echo 'p4gf_init_repo.py'

if [ $ACCOUNTNAME != $USER ]
	then
      echo 'Switching you to the new user account for Git Fusion...'
      echo 'Done'
      sudo su - $ACCOUNTNAME
fi
