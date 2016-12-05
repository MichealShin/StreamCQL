select * from s1[range unbounded],s2[range unbounded],s3[range today ts] combine(s1.id,s2.id,s3.id);
select * from s1,s2,s3 combine(s1.id,s2.id,s3.id);