# -*- mode: ruby -*-
# vi: set ft=ruby :

rake_target = ENV['TEST'] ? "test TEST=#{ENV['TEST']}" : "test"

Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.network "private_network", type: "dhcp"
  config.vm.provision "shell", inline: <<-SHELL
    sudo apt-get install software-properties-common
    sudo apt-add-repository ppa:brightbox/ruby-ng
    sudo apt-get update
    sudo apt-get install -y git ruby2.3 ruby2.3-dev
    sudo apt-get install ruby-switch
    sudo ruby-switch --set ruby2.3
    gem install bundler
    (cd /vagrant; bundle install; bundle exec rake #{rake_target})
  SHELL
end
