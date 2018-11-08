create table dir_inf(id INT NOT NULL AUTO_INCREMENT,path VARCHAR(200) NOT NULL, isDir BOOLEAN,length BIGINT,mod_Time DATETIME,owner VARCHAR(50), PRIMARY KEY (id),UNIQUE (path));


grant all privileges on migration.* to root@'10.0.68.238' identified by 'DataLake_Yonghui1';
grant all privileges on *.* to root@% identified by 'DataLake_Yonghui1';


https://blog.csdn.net/qq_24833939/article/details/79222444
flush privileges;