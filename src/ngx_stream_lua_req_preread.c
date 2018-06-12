
/*
 * Copyright (C) Xiaozhe Wang (chaoslawful)
 * Copyright (C) Yichun Zhang (agentzh)
 */


#ifndef DDEBUG
#define DDEBUG 0
#endif
#include "ddebug.h"


#include "ngx_stream_lua_util.h"
#include "ngx_stream_lua_req_preread.h"
#include "ngx_stream_lua_contentby.h"


static int ngx_stream_lua_ngx_req_preread(lua_State *L);
static void ngx_stream_lua_req_preread_handler(ngx_event_t *ev);
static void ngx_stream_lua_req_preread_cleanup(void *data);
static ngx_int_t ngx_stream_lua_req_preread_resume(ngx_stream_lua_request_t *r);


static int
ngx_stream_lua_ngx_req_preread(lua_State *L)
{
    int                          n;
    ngx_int_t                    bytes;
    size_t                       size = 0;
    off_t                        preread = 0;
    ngx_stream_lua_request_t    *r;
    luaL_Buffer luabuf;

    ngx_stream_lua_ctx_t                *ctx;
    ngx_stream_lua_co_ctx_t             *coctx;

    ctx = ngx_stream_lua_get_module_ctx(r, ngx_stream_lua_module);
    if (ctx == NULL) {
        return luaL_error(L, "no request ctx found");
    }

    ngx_stream_lua_check_context(L, ctx, NGX_STREAM_LUA_CONTEXT_PREREAD);
    n = lua_gettop(L);
    if (n != 1) {
        return luaL_error(L, "attempt to pass %d arguments, but accepted 1", n);
    }

    r = ngx_stream_lua_get_req(L);
    if (r == NULL) {
        return luaL_error(L, "no request found");
    }

    bytes = (ngx_int_t) luaL_checknumber(L, 1);

    if (bytes < 0) {
        return luaL_error(L, "invalid preread bytes \"%d\"", bytes);
    }

    // TODO: check bytes too large.

    if (r->connection->buffer != NULL) {
        preread = (size_t)ngx_buf_size(r->connection->buffer);
    }

    if (preread >= (size_t)bytes) {

        // TODO define this probe.
        ngx_stream_lua_probe_req_peak_preread(r,
                r->connection->buffer->pos,
                preread);

        luaL_buffinit(L, &luabuf);
        luaL_addlstring(&luabuf, (char *) r->connection->buffer->pos, preread);
        luaL_pushresult(&luabuf);
        return 1;
    } 

    coctx = ctx->cur_co_ctx;
    if (coctx == NULL) {
        return luaL_error(L, "no co ctx found");
    }

    ngx_stream_lua_cleanup_pending_operation(coctx);
    coctx->cleanup = ngx_stream_lua_req_preread_cleanup;
    coctx->data = r;

    r->read_event_handler = ngx_stream_lua_req_preread_handler;
    r->write_event_handler = ngx_stream_lua_core_run_phases;


    return lua_yield(L, 0);
}


void
ngx_stream_lua_req_preread_handler(ngx_event_t *ev)
{
#if (NGX_DEBUG)
    ngx_connection_t                *c;
#endif
    ngx_stream_lua_request_t        *r;
    ngx_stream_lua_ctx_t            *ctx;
    ngx_stream_lua_co_ctx_t         *coctx;

    coctx = ev->data;

    r = coctx->data;

#if (NGX_DEBUG)

    c = r->connection;

#endif

    ctx = ngx_stream_lua_get_module_ctx(r, ngx_stream_lua_module);

    if (ctx == NULL) {
        return;
    }


    coctx->cleanup = NULL;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0,
                   "stream lua preread got read event.");


    // TODO: check nbytes read satisified.

    ctx->cur_co_ctx = coctx;
    if (ctx->entered_preread_phase) {
        (void) ngx_stream_lua_req_preread_resume(r);

    } else {
        ctx->resume_handler = ngx_stream_lua_req_preread_resume;
        ngx_stream_lua_core_run_phases(r);
    }
}


void
ngx_stream_lua_inject_req_preread_api(lua_State *L)
{
    // TODO inject to ngx.req.preread()
    lua_pushcfunction(L, ngx_stream_lua_ngx_req_preread);
    lua_setfield(L, -2, "preread");
}


static void
ngx_stream_lua_req_preread_cleanup(void *data)
{
    ngx_stream_lua_co_ctx_t                *coctx = data;

   // TODO don't know what to clean yet. 
}


static ngx_int_t
ngx_stream_lua_req_preread_resume(ngx_stream_lua_request_t *r)
{
    lua_State                           *vm;
    ngx_connection_t                    *c;
    ngx_int_t                            rc;
    ngx_uint_t                           nreqs;
    ngx_stream_lua_ctx_t                *ctx;

    ctx = ngx_stream_lua_get_module_ctx(r, ngx_stream_lua_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->resume_handler = ngx_stream_lua_wev_handler;

    c = r->connection;
    vm = ngx_stream_lua_get_lua_vm(r, ctx);
    nreqs = c->requests;

    // TODO prepare retvals
    rc = ngx_stream_lua_run_thread(vm, r, ctx, 0);

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, r->connection->log, 0,
                   "lua run thread returned %d", rc);

    if (rc == NGX_AGAIN) {
        return ngx_stream_lua_run_posted_threads(c, vm, r, ctx, nreqs);
    }

    if (rc == NGX_DONE) {
        ngx_stream_lua_finalize_request(r, NGX_DONE);
        return ngx_stream_lua_run_posted_threads(c, vm, r, ctx, nreqs);
    }

    return rc;
}

/* vi:set ft=c ts=4 sw=4 et fdm=marker: */
