
create table distask(
  id serial primary key,
  command text not null,
  scheduled_at timestamp not null,
  picked_at timestamp,
  started_at timestamp,
  completed_at timestamp,
  failed_at timestamp
);

/* 
create a index on scheduled_at 
*/

create index idx_schedu_at on distask(scheduled_at);