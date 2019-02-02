// Copyright (c) 2015 Baidu, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdio.h>
#include <pthread.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <bthread/bthread.h>
#include <butil/file_util.h>                     // butil::FilePath
#include <butil/time.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/logging.h>
#include <json2pb/pb_to_json.h>
#include "json_loader.h"
#include "rpc_press_impl.h"
#include "../../../../src/proto/vse.pb.h"

using google::protobuf::Message;
using google::protobuf::Closure;

namespace pbrpcframework {

class ImportErrorPrinter
    : public google::protobuf::compiler::MultiFileErrorCollector {
public:
    // Line and column numbers are zero-based.  A line number of -1 indicates
    // an error with the entire file (e.g. "not found").
    virtual void AddError(const std::string& filename, int line,
                          int /*column*/, const std::string& message) {
        LOG_AT(ERROR, filename.c_str(), line) << message;
    }
};

int PressClient::init() {
    brpc::ChannelOptions rpc_options;
    rpc_options.connect_timeout_ms = _options->connect_timeout_ms;
    rpc_options.timeout_ms = _options->timeout_ms;
    rpc_options.max_retry = _options->max_retry;
    rpc_options.protocol = _options->protocol;
    rpc_options.connection_type = _options->connection_type;
    if (_options->attachment_size > 0) {
        _attachment.clear();
        _attachment.assign(_options->attachment_size, 'a');
    }
    if (_rpc_client.Init(_options->host.c_str(), _options->lb_policy.c_str(),
                         &rpc_options) != 0){
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }
    _method_descriptor = find_method_by_name(
        _options->service, _options->method, _importer);
    if (NULL == _method_descriptor) {
        LOG(ERROR) << "Fail to find method=" << _options->service << '.'
                   << _options->method;
        return -1;
    }
    _response_prototype = get_prototype_by_method_descriptor(
        _method_descriptor, false, _factory);
    return 0;
}

void PressClient::call_method(brpc::Controller* cntl, Message* request,
                              Message* response, Closure* done) {
    if (!_attachment.empty()) {
        cntl->request_attachment().append(_attachment);
    }
    _rpc_client.CallMethod(_method_descriptor, cntl, request, response, done);
}

RpcPress::RpcPress()
    : _pbrpc_client(NULL)
    , _ext_tid(0)
    , _started(false)
    , _stop(false)
    , _output_json(NULL) {
}

RpcPress::~RpcPress() {
    if (_output_json) {
        fclose(_output_json);
        _output_json = NULL;
    }
    delete _importer;
}

int RpcPress::init(const PressOptions* options) {
    if (NULL == options) {
        LOG(ERROR) << "Param[options] is NULL" ;
        return -1;
    }
    _options = *options;

    // Import protos.
    if (_options.proto_file.empty()) {
        LOG(ERROR) << "-proto is required";
        return -1;
    }
    int pos = _options.proto_file.find_last_of('/');
    std::string proto_file(_options.proto_file.substr(pos + 1));
    std::string proto_path(_options.proto_file.substr(0, pos));
    google::protobuf::compiler::DiskSourceTree sourceTree;
    // look up .proto file in the same directory
    sourceTree.MapPath("", proto_path.c_str());
    // Add paths in -inc
    if (!_options.proto_includes.empty()) {
        butil::StringSplitter sp(_options.proto_includes.c_str(), ';');
        for (; sp; ++sp) {
            sourceTree.MapPath("", std::string(sp.field(), sp.length()));
        }
    }
    ImportErrorPrinter error_printer;
    _importer = new google::protobuf::compiler::Importer(&sourceTree, &error_printer);
    if (_importer->Import(proto_file.c_str()) == NULL) {
        LOG(ERROR) << "Fail to import " << proto_file;
        return -1;
    }
    
    _pbrpc_client = new PressClient(&_options, _importer, &_factory);

    if (!_options.output.empty()) {
        butil::File::Error error;
        butil::FilePath path(_options.output);
        butil::FilePath dir = path.DirName();
        if (!butil::CreateDirectoryAndGetError(dir, &error)) {
            LOG(ERROR) << "Fail to create directory=`" << dir.value()
                       << "', " << error;
            return -1;
        }
        _output_json = fopen(_options.output.c_str(), "w");
        LOG_IF(ERROR, !_output_json) << "Fail to open " << _options.output;
    }

    int ret = _pbrpc_client->init();
    if (0 != ret) {
        LOG(ERROR) << "Fail to initialize rpc client";
        return ret;
    }

    if (_options.input.empty()) {
        LOG(ERROR) << "-input is empty";
        return -1;
    }
    brpc::JsonLoader json_util(_importer, &_factory, 
                                     _options.service, _options.method);

    if (butil::PathExists(butil::FilePath(_options.input))) {
        int fd = open(_options.input.c_str(), O_RDONLY);
        if (fd < 0) {
            PLOG(ERROR) << "Fail to open " << _options.input;
            return -1;
        }
        json_util.load_messages(fd, &_msgs);
    } else {
        json_util.load_messages(_options.input, &_msgs);
    }
    if (_msgs.empty()) {
        LOG(ERROR) << "Fail to load requests";
        return -1;
    }
    LOG(INFO) << "Loaded " << _msgs.size() << " requests";
    _latency_recorder.expose("rpc_press");
    _error_count.expose("rpc_press_error_count");
    return 0;
}

void* RpcPress::sync_call_thread(void* arg) {
    ((RpcPress*)arg)->sync_client();
    return NULL;
}

void* RpcPress::heart_beat_thread(void *arg) {
    ((RpcPress*)arg)->heart_beat();
    return NULL;
}

void RpcPress::handle_response(brpc::Controller* cntl,
                               Message* request,
                               Message* response,
                               int64_t start_time){
    if (!cntl->Failed()){
        int64_t rpc_call_time_us = butil::gettimeofday_us() - start_time;
        _latency_recorder << rpc_call_time_us;

        if (_output_json) {
            std::string response_json;
            std::string error;
            if (!json2pb::ProtoMessageToJson(*response, &response_json, &error)) {
                LOG(WARNING) << "Fail to convert to json: " << error;
            }
            fprintf(_output_json, "%s\n", response_json.c_str());
        }
    } else {
        LOG(WARNING) << "error_code=" <<  cntl->ErrorCode() << ", "
                   << cntl->ErrorText();
        _error_count << 1;
    }
    delete response;
    delete cntl;
}

class EWMAStatistic{

public:

    float avg_value=0;
    float alpha = 0.8;

    float update(float sample){
        avg_value = alpha * sample + (1-alpha) * avg_value;
        return avg_value;
    }

    float get(){
        return avg_value;
    }
};

class PIDControl{
public:
    float kt = 1;
    float kp = 0.4*kt; // the bigger kp is, the more quickly change is
    float ki = 0*kt;// the smaller ki is, the more stable output is
    float kd = 0*kt;  // should not too big,
    float error_pre_pre = 0;
    float error_pre = 0;
    float error_cur = 0;
    float u_pre = 0;
    float u_cur = 0;
    float delta_u = 0;
    int update_index = 0;

    inline bool update_u(float error_sample){
        error_pre_pre = error_pre;
        error_pre = error_cur;
        error_cur = error_sample;
        ++update_index;

        if(update_index<3)
            return false;

        u_pre = u_cur;
        delta_u = kp * (error_cur - error_pre) + ki * error_cur + kd * (error_cur - 2*error_pre + error_pre_pre);
        u_cur = u_pre + delta_u;

        return true;
    }

    float get(){
        return delta_u;
    }
};


class PIDControlV2{
public:
    float kp = 0;

    float t = 0.6;
    float ti = 1;
    float ki = kp * t / ti;

    float td = 0;
    float kd = kp * td / t;

    float u_cur = 0;
    float u_pre = 0;
    float u_delta = 0;

    float error_pre = 0;
    float error_cur = 0;
    float error_sum = 0;

    inline bool update_u(float error_sample){
        error_pre = error_cur;
        error_cur = error_sample;
        error_sum += error_sample;
        kp = 0.04;
        ki = 0.0003;
        kd = 0.0003;
        u_pre = u_cur;
        u_cur = kp * error_cur + ki * error_sum + kd * (error_cur - error_pre);
        u_delta = u_cur - u_pre;

        return true;
    }

    float get(){
        return u_cur;
    }

};

static PIDControlV2 pid_cntl;

void RpcPress::heartbeat_responsehandle(brpc::Controller* cntl,
                                  google::protobuf::Message* request,
                                  google::protobuf::Message* response,
                                  int64_t start_time_ns){
//    LOG(INFO)<<"GET heart beat response";
    ::dg::model::vse::HeartbeatResponse *res = (::dg::model::vse::HeartbeatResponse *)response;
    if(res->code() == 103){
        LOG(WARNING)<<"task not exists.";
    }else{
//        LOG(INFO)<<"get success qps: "<<res->success_qps();
//        LOG(INFO)<<"get limit qps: "<<res->limit_qps();
//        LOG(INFO)<<"get call qps: "<<res->call_qps();
//        LOG(INFO)<<"get pidlimit qps: "<<res->pidlimit_qps();
//        LOG(INFO)<<"get buffer max limit: "<<res->buffer_maxlimit();
//        LOG(INFO)<<"get buffer current: "<<res->buffer_current();
//        LOG(INFO)<<"get buffer window current: "<<res->buffer_windowdura();
//        LOG(INFO)<<"get lateset buffer window by limit: "<<res->buffer_reachlimit_windowdura();
//        LOG(INFO)<<"get buffer oneframe latency: "<<res->buffer_oneframe_latency();

//        if(res->buffer_maxlimit()<=res->buffer_current() ){
//
//            _sleep_time.store( 1.0f/(res->success_qps()*1.0f)*1000.0f*1000.0f );
//            usleep(_sleep_time);
//        }

//        pid_cntl.update_u( res->buffer_current() - res->buffer_maxlimit() );
//
//        _options.rate_mtx->lock();
//        _options.test_req_rate -=  pid_cntl.get();
//        _options.rate_mtx->unlock();
//
//        static EWMAStatistic set_qps;
//        static EWMAStatistic error_avg;
//        static EWMAStatistic buffer_window_avg;
//        static EWMAStatistic actual_qps_avg;
//        static EWMAStatistic target_qps_avg;
//        static EWMAStatistic buffer_count_avg;
//
//        if(res->buffer_windowdura() > 0){
//            buffer_window_avg.update(res->buffer_windowdura()*1.0f);
//            actual_qps_avg.update(res->buffer_current()*1000.0f/res->buffer_windowdura());
//
//            target_qps_avg.update(  res->buffer_maxlimit()/(res->buffer_windowdura()*1.0f/res->buffer_current()*res->buffer_maxlimit()) );
//        }
//        LOG(INFO)<<"window_avg: "<<buffer_window_avg.get();
//        LOG(INFO)<<"actual_qps: "<<actual_qps_avg.get();
//        LOG(INFO)<<"target_qps: "<<target_qps_avg.get();

//        error_avg.update(res->buffer_maxlimit() -res->buffer_current());
//
//        LOG(INFO)<<"error avg: "<<error_avg.get();
//        if( pid_cntl.update_u( error_avg.get() ) ){
//            set_qps.update(_options.test_req_rate + pid_cntl.delta_u);
//            _options.test_req_rate = set_qps.get();
//            LOG(INFO)<<"reset qps: "<<this->_options.test_req_rate;
//            usleep(1000*1000);
////            usleep(2.0f * 1.0f/_options.test_req_rate * 1000.0f * 1000.0f);
//
////            _options.test_req_rate  = (res->buffer_current() + pid_cntl.delta_u)*1.0f * _options.test_req_rate*1.0f / (res->buffer_current()*1.0f) ;
//        }
//
//        LOG(INFO)<<"reset qps: "<<this->_options.test_req_rate;
//        LOG(INFO)<<"delta u: "<<pid_cntl.u_delta;
//        LOG(INFO)<<"u_cur: "<<pid_cntl.u_cur;
//
//        {
//            std::unique_lock<std::mutex> lk(*_options.cv_mtx);
//            _options.cv->wait(lk);
//        }
        usleep(2*1000*1000);
    }
}


static butil::atomic<int> g_thread_count(0);

void RpcPress::heart_beat(){

    ::dg::model::vse::HeartbeatRequest h_req;
    h_req.set_task_id(((::dg::model::vse::PushImageRequest *)(_msgs.front()))->task_id());

    ::dg::model::vse::VSEService_Stub stub_(&_pbrpc_client->_rpc_client);
    int64_t sleep_ms = 1000;

    while (!_stop){
        const int64_t start_time = butil::gettimeofday_us();

        brpc::Controller cntl;

        ::dg::model::vse::HeartbeatResponse h_res;

        google::protobuf::Closure* done = brpc::NewCallback<
                RpcPress,
                RpcPress*,
                brpc::Controller*,
                Message*,
                Message*, int64_t>
                (this, &RpcPress::heartbeat_responsehandle, &cntl, &h_req, &h_res, start_time);

        const brpc::CallId cid1 = cntl.call_id();

        stub_.Heartbeat(&cntl, &h_req, &h_res, done);

        brpc::Join(cid1);

        usleep(sleep_ms*1000);
    }
}

void RpcPress::sync_client_v2() {
    //max make up time is 5 s
    if (_msgs.empty()) {
        LOG(ERROR) << "nothing to send!";
        return;
    }
    const int thread_index = g_thread_count.fetch_add(1, butil::memory_order_relaxed);
    int msg_index = thread_index;
    _sleep_time.store(1000*1000 * (1.0f/this->_options.test_req_rate));

    while (!_stop) {
        brpc::Controller* cntl = new brpc::Controller;
        msg_index = (msg_index + _options.test_thread_num) % _msgs.size();
        Message* request = _msgs[msg_index];
        Message* response = _pbrpc_client->get_output_message();
        google::protobuf::Closure* done = brpc::NewCallback<
                RpcPress,
                RpcPress*,
                brpc::Controller*,
                Message*,
                Message*, int64_t>
                (this, &RpcPress::handle_response, cntl, request, response, 0);
        const brpc::CallId cid1 = cntl->call_id();
        _pbrpc_client->call_method(cntl, request, response, done);
        _sent_count << 1;

        if (_options.test_req_rate <= 0) {
            brpc::Join(cid1);
        } else {
            LOG(INFO)<<"sleep time: "<<_sleep_time.load();
            usleep(_sleep_time.load());
        }
    }
}
void RpcPress::sync_clientV3() {

    //max make up time is 5 s
    if (_msgs.empty()) {
        LOG(ERROR) << "nothing to send!";
        return;
    }
    const int thread_index = g_thread_count.fetch_add(1, butil::memory_order_relaxed);
    int msg_index = thread_index;
    std::deque<int64_t> timeq;

    timeq.push_back(butil::gettimeofday_us());
    while (!_stop) {

        _options.rate_mtx->lock();
        double req_rate = _options.test_req_rate / _options.test_thread_num;
        size_t MAX_QUEUE_SIZE = (size_t)req_rate;
        if (MAX_QUEUE_SIZE < 100) {
            MAX_QUEUE_SIZE = 100;
        } else if (MAX_QUEUE_SIZE > 2000) {
            MAX_QUEUE_SIZE = 2000;
        }

        brpc::Controller* cntl = new brpc::Controller;
        msg_index = (msg_index + _options.test_thread_num) % _msgs.size();
        Message* request = _msgs[msg_index];
        Message* response = _pbrpc_client->get_output_message();
        const int64_t start_time = butil::gettimeofday_us();
        google::protobuf::Closure* done = brpc::NewCallback<
            RpcPress, 
            RpcPress*, 
            brpc::Controller*, 
            Message*, 
            Message*, int64_t>
            (this, &RpcPress::handle_response, cntl, request, response, start_time);
        const brpc::CallId cid1 = cntl->call_id();
        _pbrpc_client->call_method(cntl, request, response, done);
        _sent_count << 1;

        if (_options.test_req_rate <= 0) { 
            brpc::Join(cid1);
            _options.rate_mtx->unlock();
        } else {
            int64_t end_time = butil::gettimeofday_us();
            int64_t expected_elp = 0;
            int64_t actual_elp = 0;
            timeq.push_back(end_time);
            if (timeq.size() > MAX_QUEUE_SIZE) {
                actual_elp = end_time - timeq.front();
                timeq.pop_front();
                expected_elp = (int64_t)(1000000 * timeq.size() / req_rate);
            } else {
                actual_elp = end_time - timeq.front();
                expected_elp = (int64_t)(1000000 * (timeq.size() - 1) / req_rate);
            }
            if (actual_elp < expected_elp){
                _options.rate_mtx->unlock();
                int  slp = expected_elp - actual_elp  ;
                usleep(slp);
            }else{
                _options.rate_mtx->unlock();
                _options.cv->notify_one();
            }
        }

    }
}

void RpcPress::sync_client() {

    //max make up time is 5 s
    if (_msgs.empty()) {
        LOG(ERROR) << "nothing to send!";
        return;
    }
    const int thread_index = g_thread_count.fetch_add(1, butil::memory_order_relaxed);
    int msg_index = thread_index;
    std::deque<int64_t> timeq;

    double req_rate = _options.test_req_rate / _options.test_thread_num;
    size_t MAX_QUEUE_SIZE = (size_t)req_rate;
    if (MAX_QUEUE_SIZE < 100) {
        MAX_QUEUE_SIZE = 100;
    } else if (MAX_QUEUE_SIZE > 2000) {
        MAX_QUEUE_SIZE = 2000;
    }

    timeq.push_back(butil::gettimeofday_us());
    while (!_stop) {

        brpc::Controller* cntl = new brpc::Controller;
        msg_index = (msg_index + _options.test_thread_num) % _msgs.size();
        Message* request = _msgs[msg_index];
        Message* response = _pbrpc_client->get_output_message();
        const int64_t start_time = butil::gettimeofday_us();
        google::protobuf::Closure* done = brpc::NewCallback<
                RpcPress,
                RpcPress*,
                brpc::Controller*,
                Message*,
                Message*, int64_t>
                (this, &RpcPress::handle_response, cntl, request, response, start_time);
        const brpc::CallId cid1 = cntl->call_id();
        _pbrpc_client->call_method(cntl, request, response, done);
        _sent_count << 1;

        if (_options.test_req_rate <= 0) {
            brpc::Join(cid1);
            _options.rate_mtx->unlock();
        } else {
            int64_t end_time = butil::gettimeofday_us();
            int64_t expected_elp = 0;
            int64_t actual_elp = 0;
            timeq.push_back(end_time);
            if (timeq.size() > MAX_QUEUE_SIZE) {
                actual_elp = end_time - timeq.front();
                timeq.pop_front();
                expected_elp = (int64_t)(1000000 * timeq.size() / req_rate);
            } else {
                actual_elp = end_time - timeq.front();
                expected_elp = (int64_t)(1000000 * (timeq.size() - 1) / req_rate);
            }
            if (actual_elp < expected_elp){
                usleep(expected_elp - actual_elp);
            }
        }

    }
}

int RpcPress::start() {
    int ret = 0;

    if(FLAGS_task_type=="image-stream"){
        if ((ret = pthread_create(&_ext_tid, NULL, heart_beat_thread, this)) != 0) {
            LOG(ERROR) << "Fail to create heart beat thread";
            return -1;
        }
    }

    _ttid.resize(_options.test_thread_num);

    for (int i = 0; i < _options.test_thread_num; i++) {
        if ((ret = pthread_create(&_ttid[i], NULL, sync_call_thread, this)) != 0) {
            LOG(ERROR) << "Fail to create sending threads";
            return -1;
        }
    }
    brpc::InfoThreadOptions info_thr_opt;
    info_thr_opt.latency_recorder = &_latency_recorder;
    info_thr_opt.error_count = &_error_count;
    info_thr_opt.sent_count = &_sent_count;
    if (!_info_thr.start(info_thr_opt)) {
        LOG(ERROR) << "Fail to create stats thread";
        return -1;
    }
    _started = true;


    return 0;
}
int RpcPress::stop() {
    if (!_started) {
        return -1;
    }
    _stop = true;
    for (size_t i = 0; i < _ttid.size(); i++) {
        pthread_join(_ttid[i], NULL);
    }

    if(FLAGS_task_type=="image-stream"){
        pthread_join(_ext_tid, NULL);
    }
    _info_thr.stop();
    return 0;
}
} //namespace
