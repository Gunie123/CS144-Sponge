#include "tcp_sender.hh"

#include "tcp_config.hh"
#include "wrapping_integers.hh"

#include <random>
#include <string>

using namespace std;

//! \param[in] capacity the capacity of the outgoing byte stream
//! \param[in] retx_timeout the initial amount of time to wait before retransmitting the oldest outstanding segment
//! \param[in] fixed_isn the Initial Sequence Number to use, if set (otherwise uses a random ISN)
TCPSender::TCPSender(const size_t capacity, const uint16_t retx_timeout, const std::optional<WrappingInt32> fixed_isn)
    : _isn(fixed_isn.value_or(WrappingInt32{random_device()()}))
    , _initial_retransmission_timeout{retx_timeout}
    , _stream(capacity) {
    _retx_timer = RetransmissionTimer(_initial_retransmission_timeout);
}

uint64_t TCPSender::bytes_in_flight() const { return _bytes_in_flight; }

void TCPSender::fill_window() {
    // 窗口为 0 时视为 1（零窗口探测）
    uint16_t effective_window = _window_size > 0 ? _window_size : 1;

    while (bytes_in_flight() < effective_window) {
        TCPSegment seg;

        // 1. 如果 SYN 尚未发送，设置 SYN 标志
        if (!_syn_sent) {
            seg.header().syn = true;
            _syn_sent = true;
        }

        // 当前段已占用的序列空间（SYN 占 1）
        size_t seg_seq_len = seg.header().syn ? 1 : 0;

        // 2. 计算还能发送多少字节的 payload
        size_t room = effective_window - bytes_in_flight();
        size_t payload_len = min(static_cast<size_t>(TCPConfig::MAX_PAYLOAD_SIZE), _stream.buffer_size());
        payload_len = min(payload_len, room - seg_seq_len);

        // 3. 从 ByteStream 读取数据
        if (payload_len > 0) {
            string data = _stream.read(payload_len);
            seg.payload() = Buffer(move(data));
        }
        seg_seq_len += seg.payload().size();

        // 4. 如果流已结束、所有数据已读完、且窗口有空间，设置 FIN 标志
        if (!_fin_sent && _stream.eof() && _stream.buffer_size() == 0 && seg_seq_len < room) {
            seg.header().fin = true;
            _fin_sent = true;
            seg_seq_len++;
        }

        // 5. 如果此段不占任何序列空间（无 SYN、无数据、无 FIN），停止填充
        if (seg_seq_len == 0)
            break;

        // 6. 设置序列号
        seg.header().seqno = wrap(_next_seqno, _isn);
        _next_seqno += seg_seq_len;

        // 7. 推入发送队列
        _segments_out.push(seg);

        // 8. 如果段占用序列空间，加入未完成队列，并启动计时器
        if (seg.length_in_sequence_space() > 0) {
            _outstanding_segments.push({_next_seqno - seg_seq_len, seg});
            _bytes_in_flight += seg.length_in_sequence_space();
            if (!_retx_timer.is_running()) {
                _retx_timer.start();
            }
        }
    }
}

//! \param ackno The remote receiver's ackno (acknowledgment number)
//! \param window_size The remote receiver's advertised window size
void TCPSender::ack_received(const WrappingInt32 ackno, const uint16_t window_size) {
    uint64_t abs_ackno = unwrap(ackno, _isn, _next_seqno);

    // 忽略超出已发送范围的 ackno
    if (abs_ackno > _next_seqno)
        return;

    _window_size = window_size;

    // 判断是否有新数据被确认
    bool new_data_acked = (abs_ackno > _last_abs_ackno);

    // 移除已完全确认的未完成段（ackno 覆盖了段的所有序列号）
    while (!_outstanding_segments.empty()) {
        auto &[abs_seq, seg] = _outstanding_segments.front();
        if (abs_seq + seg.length_in_sequence_space() <= abs_ackno) {
            _bytes_in_flight -= seg.length_in_sequence_space();
            _outstanding_segments.pop();
        } else {
            break;
        }
    }

    // 有新数据被确认：重置 RTO 和连续重传计数
    if (new_data_acked) {
        _last_abs_ackno = abs_ackno;
        _consecutive_retransmissions = 0;
        _retx_timer.reset_rto();
        if (!_outstanding_segments.empty()) {
            _retx_timer.start();
        } else {
            _retx_timer.stop();
        }
    }

    // 新空间释放后尝试填充窗口
    fill_window();
}

//! \param[in] ms_since_last_tick the number of milliseconds since the last call to this method
void TCPSender::tick(const size_t ms_since_last_tick) {
    if (!_retx_timer.tick(ms_since_last_tick))
        return;  // 未超时，什么也不做

    // 计时器超时：重传最早的未完成段
    if (!_outstanding_segments.empty()) {
        _segments_out.push(_outstanding_segments.front().second);
    }

    // 窗口非零：指数退避 + 增加连续重传计数
    if (_window_size > 0) {
        _consecutive_retransmissions++;
        _retx_timer.backoff_and_restart();
    } else {
        // 窗口为零：仅重启计时器，不翻倍 RTO，不增加重传计数
        _retx_timer.start();
    }
}

unsigned int TCPSender::consecutive_retransmissions() const { return _consecutive_retransmissions; }

void TCPSender::send_empty_segment() {
    TCPSegment seg;
    seg.header().seqno = wrap(_next_seqno, _isn);
    _segments_out.push(seg);
    // 注意：空段不占序列空间，不加入 _outstanding_segments，不启动计时器
}
