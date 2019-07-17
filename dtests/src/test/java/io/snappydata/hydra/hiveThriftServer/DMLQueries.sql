 update default.Student set marks = marks + 100;
 update default.Student set subject = 'Multithreading and Concurrency' where subject='Maths-2';
  select id,subject,marks from default.Student;
 update default.Student set marks = marks - 200;
 update default.Student set subject = 'Maths-2' where subject='Multithreading and Concurrency';
 select * from default.Student;
 delete from default.Student where subject = 'Graphics';






