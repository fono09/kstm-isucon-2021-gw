#!/bin/bash
mysql -u root -pishocon1 ishocon1 < ~/.init/init.sql
ruby ~/.init/insert.rb
