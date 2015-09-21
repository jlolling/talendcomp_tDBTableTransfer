create table db_type_test_source (
    int_val integer,
    char_val varchar2(16),
    clob_val clob,
    blob_val blob,
    date_val date,
    num_val number(22,4),
    constraint db_type_test_source_pk primary key (int_val));
    
create table db_type_test_target (
    int_val integer,
    char_val varchar2(16),
    clob_val clob,
    blob_val blob,
    date_val date,
    num_val number(22,4),
    constraint db_type_test_target_pk primary key (int_val));
