 update default.Student set marks = marks + 100;
 update default.Student set subject = 'Multithreading and Concurrency' where tid < 5;
 select id,subject,marks from default.Student order by id ASC;
 update default.Student set marks = marks - 200;
 delete from default.Student where tid > 6;
 select * from default.Student;
 delete from default.Student where subject = 'Graphics' and tid = 8;