//
// Created by Stefan on 16/03/2023.
//

#include "query.h"
#include <stdlib.h>
#include <string.h>

condition* create_integer_condition(char* name, enum relation relation, int32_t val){
    condition* c = malloc(sizeof(condition));
    c->col_name = name;
    c->type = INTEGER;
    c->size = sizeof(int32_t);
    c->relation = relation;
    c->value = malloc(c->size);
    c->value->i = val;
    c->next = NULL;
    return c;
}

column_to_update* create_integer_column_to_update(char* name, int32_t val) {

    column_to_update* c = malloc(sizeof(column_to_update));
    c->col_name = name;
    c->type = INTEGER;
    c->size = sizeof(int32_t);
    c->value = malloc(c->size);
    c->value->i = val;
    c->next = NULL;
    return c;
}

bool check_integers(int32_t first, int32_t second, enum relation rel){
    switch (rel) {
        case LESS:
            if (first < second){
                return true;
            }
            break;
        case LESS_EQUAL:
            if (first <= second){
                return true;
            }
            break;
        case MORE:
            if (first > second){
                return true;
            }
            break;
        case MORE_EQUAL:
            if (first >= second){
                return true;
            }
            break;
        case EQUAL:
            if (first == second){
                return true;
            }
            break;
        case NOT_EQUAL:
            if (first != second){
                return true;
            }
            break;
        default:
            return false;
    }
    return false;
}

bool check_floats(float first, float second, enum relation rel){
    switch (rel) {
        case LESS:
            if (first < second){
                return true;
            }
            break;
        case LESS_EQUAL:
            if (first <= second){
                return true;
            }
            break;
        case MORE:
            if (first > second){
                return true;
            }
            break;
        case MORE_EQUAL:
            if (first >= second){
                return true;
            }
            break;
        case EQUAL:
            if (first == second){
                return true;
            }
            break;
        case NOT_EQUAL:
            if (first != second){
                return true;
            }
            break;
        default:
            return false;
    }
    return false;
}

bool check_bools(bool first, bool second, enum relation rel){
    switch (rel) {
        case EQUAL:
            if (first == second){
                return true;
            }
            break;
        case NOT_EQUAL:
            if (first != second){
                return true;
            }
            break;
        default:
            return false;
    }
    return false;
}

bool check_strings(char* first, char* second, enum relation rel){
    switch (rel) {
        case LESS:
            if (strlen(first) < strlen(second)){
                return true;
            }
            break;
        case LESS_EQUAL:
            if (strlen(first) <= strlen(second)){
                return true;
            }
            break;
        case MORE:
            if (strlen(first) > strlen(second)){
                return true;
            }
            break;
        case MORE_EQUAL:
            if (strlen(first) >= strlen(second)){
                return true;
            }
            break;
        case EQUAL:
            if (!strcmp(first, second)){
                return true;
            }
            break;
        case NOT_EQUAL:
            if (strcmp(first, second)){
                return true;
            }
            break;
        default:
            return false;
    }
    return false;
}

void select_records_from_table(uint8_t num_cols, char** view_cols, condition* cond, table* tb, database* db, FILE* f) {

    page* curr_pg = allocate_page(db->block_size);
    read_page_from_file(f, curr_pg, tb->first_record_block_id);
    block* curr_blk = read_block_from_page(curr_pg);

    uint32_t off = sizeof(block);

    char* string = malloc(2);

    for(uint32_t read_rows=0; read_rows < tb->num_rows; read_rows++){
        if(off >= curr_blk->offset){
            off = sizeof(block);
            get_next_block(curr_blk->next_id, curr_blk, curr_pg, f);
        }
        record* r = read_record_from_page(tb, curr_pg, off);
        off += get_columns_size(tb) + sizeof(bool);

        memcpy(string, "|", 2);
        for (int i = 0; i < num_cols; i++) {
            column* c = get_column_by_name(tb, view_cols[i]);
            switch (c->type) {
                case INTEGER:
                    string = integer_to_string(get_integer_record(tb, r, c->name), string);
                    break;
                case FLOAT:
                    string = float_to_string(get_float_record(tb, r, c->name), string);
                    break;
                case BOOL:
                    string = bool_to_string(get_boolean_record(tb, r, c->name), string);
                    break;
                case STRING:;
                    char* temp = get_string_record(tb, r, c->name);
                    string = string_to_string(temp, string);
                    free(temp);
                    break;
            }
        }

        bool flag = true;
        condition* temp = cond;
        while(temp != NULL) {

            switch (temp->type) {
                case INTEGER:;
                    int32_t i_n = get_integer_record(tb, r, temp->col_name);
                    if(!check_integers(i_n, temp->value->i, temp->relation)){
                        flag = false;
                    }
                    break;
                case FLOAT:;
                    float f_n = get_float_record(tb, r, temp->col_name);
                    if(!check_floats(f_n, temp->value->f, temp->relation)){
                        flag = false;
                    }
                    break;
                case BOOL:;
                    bool b_n = get_boolean_record(tb, r, temp->col_name);
                    if(!check_bools(b_n, temp->value->b, temp->relation)){
                        flag = false;
                    }
                    break;
                case STRING:;
                    char* s = get_string_record(tb, r, temp->col_name);
                    if(!check_strings(s, temp->value->s, temp->relation)){
                        flag = false;
                    }
                    break;
            }

            if(!flag) {
                break;
            }
            temp = temp->next;
        }

        if(flag) {
            //printf(string);
            //printf("\n");
        }

    }
    free(string);
}

void update_records_in_table(column_to_update* col, condition* cond, table* tb, database* db, FILE* f) {
    page* curr_pg = allocate_page(db->block_size);
    read_page_from_file(f, curr_pg, tb->first_record_block_id);
    block* curr_blk = read_block_from_page(curr_pg);

    uint32_t off = sizeof(block);

    for(uint32_t read_rows=0; read_rows < tb->num_rows; read_rows++){
        if(off >= curr_blk->offset){
            off = sizeof(block);
            get_next_block(curr_blk->next_id, curr_blk, curr_pg, f);
        }
        record* r = read_record_from_page(tb, curr_pg, off);

        bool flag = true;
        condition* temp = cond;
        while(temp != NULL) {

            switch (temp->type) {
                case INTEGER:;
                    int32_t i_n = get_integer_record(tb, r, temp->col_name);
                    if(!check_integers(i_n, temp->value->i, temp->relation)){
                        flag = false;
                    }
                    break;
                case FLOAT:;
                    float f_n = get_float_record(tb, r, temp->col_name);
                    if(!check_floats(f_n, temp->value->f, temp->relation)){
                        flag = false;
                    }
                    break;
                case BOOL:;
                    bool b_n = get_boolean_record(tb, r, temp->col_name);
                    if(!check_bools(b_n, temp->value->b, temp->relation)){
                        flag = false;
                    }
                    break;
                case STRING:;
                    char* s = get_string_record(tb, r, temp->col_name);
                    if(!check_strings(s, temp->value->s, temp->relation)){
                        flag = false;
                    }
                    break;
            }

            if(!flag) {
                break;
            }
            temp = temp->next;
        }

        if(flag) {
            /* Update */
            column_to_update* temp_col = col;
            while(temp_col != NULL) {

                switch (temp_col->type) {
                    case INTEGER:
                        insert_integer_record(tb, r, temp_col->col_name,temp_col->value->i);
                        break;
                    case FLOAT:
                        insert_float_record(tb, r, temp_col->col_name,temp_col->value->f);
                        break;
                    case BOOL:
                        insert_boolean_record(tb, r, temp_col->col_name,temp_col->value->b);
                        break;
                    case STRING:
                        insert_string_record(tb, r, temp_col->col_name,temp_col->value->s);
                        break;
                }

                temp_col = temp_col->next;
            }
            write_record_to_page(r, curr_pg, off, get_columns_size(tb));
            write_page_to_file(f, curr_pg, curr_blk->id);

        }
        off += get_columns_size(tb);
    }

}

void delete_records_from_table(condition* cond, table* tb, database* db, FILE* f) {

    uint32_t pre_id = tb->first_record_block_id;

    page* curr_pg = allocate_page(db->block_size);
    read_page_from_file(f, curr_pg, tb->first_record_block_id);
    block* curr_blk = read_block_from_page(curr_pg);

    uint32_t next_id = curr_blk->next_id;

    page* last_pg = allocate_page(db->block_size);
    read_page_from_file(f, last_pg, tb->last_record_block_id);
    block* last_blk = read_block_from_page(last_pg);

    uint32_t deleted_rows = 0;
    uint32_t off = sizeof(block);

    bool block_flag = false;

    for(uint32_t i = 0; i < tb->num_rows; i++){

        record* r = read_record_from_page(tb, curr_pg, off);

        bool flag = true;
        condition* temp = cond;
        while(temp != NULL) {

            switch (temp->type) {
                case INTEGER:;
                    int32_t i_n = get_integer_record(tb, r, temp->col_name);
                    if(!check_integers(i_n, temp->value->i, temp->relation)){
                        flag = false;
                    }
                    break;
                case FLOAT:;
                    float f_n = get_float_record(tb, r, temp->col_name);
                    if(!check_floats(f_n, temp->value->f, temp->relation)){
                        flag = false;
                    }
                    break;
                case BOOL:;
                    bool b_n = get_boolean_record(tb, r, temp->col_name);
                    if(!check_bools(b_n, temp->value->b, temp->relation)){
                        flag = false;
                    }
                    break;
                case STRING:;
                    char* s = get_string_record(tb, r, temp->col_name);
                    if(!check_strings(s, temp->value->s, temp->relation)){
                        flag = false;
                    }
                    break;
            }

            if(!flag) {
                break;
            }
            temp = temp->next;
        }

        if(flag) {
            block_flag = true;
            r->valid = false;
            write_record_to_page(r, curr_pg, off, get_columns_size(tb));
            deleted_rows++;
        }
        off += get_columns_size(tb);

        if(off >= curr_blk->offset || i+1>=tb->num_rows){
            if(block_flag){
                if(pre_id != curr_blk->id) {
                    page* pre_pg = allocate_page(db->block_size);
                    read_page_from_file(f, pre_pg, pre_id);
                    block* pre_blk = read_block_from_page(pre_pg);

                    pre_blk->next_id = curr_blk->next_id;

                    write_block_to_page(pre_blk, pre_pg);
                    write_page_to_file(f, pre_pg, pre_id);

                    free(pre_blk);
                    free(pre_pg);
                } else {
                    tb->first_record_block_id = curr_blk->next_id;
                }
                next_id = curr_blk->next_id;
                write_to_last_record_block(f,
                                           curr_pg,
                                           curr_blk,
                                           last_pg,
                                           last_blk,
                                           tb,
                                           db);

                block_flag = false;
            }

            off = sizeof(block);
            pre_id = curr_blk->id;
            get_next_block(next_id, curr_blk,curr_pg, f);
        }

    }
    tb->num_rows -= deleted_rows;

}