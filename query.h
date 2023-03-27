//
// Created by Stefan on 16/03/2023.
//

#ifndef LABDB_QUERY_H
#define LABDB_QUERY_H

#include "pager.h"
#include "database.h"
#include "table.h"

typedef struct query query;
typedef struct join_query join_query;
typedef struct condition condition;
typedef struct column_to_update column_to_update;
typedef struct table_to_join table_to_join;

union type_value;


enum query_type {SELECT, INSERT, UPDATE, DELETE};
enum relation {EQUAL, NOT_EQUAL, LESS, MORE, LESS_EQUAL, MORE_EQUAL};


struct query {
    enum query_type type;
    char** viewCols;
    char* table;
    condition* condition;
};

struct table_to_join {
    table* tb;
    uint8_t num_cols;
    char** view_cols;
    condition* cond;

    column* col_to_join;
};

struct column_to_update {
    char* col_name;
    enum data_type type;
    uint32_t size;
    union type_value* value;

    column_to_update* next;
};

struct condition {
    char* col_name;
    enum data_type type;
    uint32_t size;
    enum relation relation;
    union type_value* value;

    condition* next;
};

union type_value {
    int32_t i;
    float f;
    bool b;
    char* s;
};

uint32_t select_records_from_table(uint32_t block_offset, char* buffer, uint32_t buff_sz, uint8_t num_cols, char** view_cols, condition* cond,
                                   table* tb, database* db, FILE* f);
void update_records_in_table(column_to_update* col, condition* cond, table* tb, database* db, FILE* f);
void delete_records_from_table(condition* cond, table* tb, database* db, FILE* f);
void select_records_from_table_inner_join(uint32_t* left_block_off, uint32_t* right_block_off, char* buffer, uint32_t buff_sz,
                                          table_to_join* left, table_to_join* right, database* db, FILE* f);

condition* create_integer_condition(char* name, enum relation relation, int32_t val);
condition* create_float_condition(char* name, enum relation relation, float val);
condition* create_bool_condition(char* name, enum relation relation, bool val);
condition* create_string_condition(char* name, enum relation relation, char* val);

column_to_update* create_integer_column_to_update(char* name, int32_t val);
column_to_update* create_float_column_to_update(char* name, float val);
column_to_update* create_bool_column_to_update(char* name, bool val);
column_to_update* create_string_column_to_update(char* name, char* val);

table_to_join* create_table_to_join(table* tb,
                                    uint8_t num_cols,
                                    char** view_cols,
                                    condition* cond,
                                    column* col_to_join);

#endif //LABDB_QUERY_H
