#ifndef SPONGE_LIBSPONGE_TCP_SENDER_HH
#define SPONGE_LIBSPONGE_TCP_SENDER_HH

#include "byte_stream.hh"
#include "tcp_config.hh"
#include "tcp_segment.hh"
#include "wrapping_integers.hh"

#include <functional>
#include <queue>

//! \brief 重传计时器 —— 管理 RTO、超时检测和指数退避
class RetransmissionTimer {
  private:
    unsigned int _initial_rto;  // 初始重传超时（毫秒），永远不变
    unsigned int _current_rto;  // 当前重传超时（毫秒），超时时翻倍
    size_t _elapsed_time{0};    // 计时器启动以来累计的毫秒数
    bool _is_running{false};    // 计时器是否在运行

  public:
    //! \brief 用初始 RTO 构造计时器
    explicit RetransmissionTimer(unsigned int initial_rto) : _initial_rto(initial_rto), _current_rto(initial_rto) {}

    //! \brief 启动计时器（重置已流逝时间）
    void start() {
        _is_running = true;
        _elapsed_time = 0;
    }

    //! \brief 停止计时器
    void stop() {
        _is_running = false;
        _elapsed_time = 0;
    }
    //! \brief 仅重置 RTO 不重启
    void reset_rto() { _current_rto = _initial_rto; }

    //! \brief 时间流逝。返回 true 表示计时器超时
    bool tick(size_t ms_since_last_tick) {
        if (!_is_running)
            return false;
        _elapsed_time += ms_since_last_tick;
        return _elapsed_time >= _current_rto;
    }

    //! \brief 超时后：将 RTO 翻倍（指数退避）并重启计时器
    void backoff_and_restart() {
        _current_rto *= 2;
        start();  // 重置 elapsed_time = 0，保持运行
    }

    //! \brief 收到有效 ACK 时：将 RTO 重置为初始值并重启（如果有未完成数据）
    void reset_and_restart() {
        reset_rto();
        start();
    }

    //! \brief 是否正在运行
    bool is_running() const { return _is_running; }
};

//! \brief The "sender" part of a TCP implementation.

//! Accepts a ByteStream, divides it up into segments and sends the
//! segments, keeps track of which segments are still in-flight,
//! maintains the Retransmission Timer, and retransmits in-flight
//! segments if the retransmission timer expires.
class TCPSender {
  private:
    //! our initial sequence number, the number for our SYN.
    WrappingInt32 _isn;

    //! outbound queue of segments that the TCPSender wants sent
    std::queue<TCPSegment> _segments_out{};

    //! retransmission timer for the connection
    unsigned int _initial_retransmission_timeout;

    //! outgoing stream of bytes that have not yet been sent
    ByteStream _stream;

    //! the (absolute) sequence number for the next byte to be sent
    uint64_t _next_seqno{0};

    // ! 重传计时器
    RetransmissionTimer _retx_timer{_initial_retransmission_timeout};

    // ！连续重传次数
    unsigned int _consecutive_retransmissions{0};

    // ！已发送但未确认的段，按发送顺序存储（绝对序列号， TCPSegment）
    std::queue<std::pair<uint64_t, TCPSegment>> _outstanding_segments{};

    // ！未完成段的序列空间总字节数（缓存值，避免遍历 queue）
    uint64_t _bytes_in_flight{0};

    // ！上次收到的最大绝对确认号
    uint64_t _last_abs_ackno{0};

    // ！发送窗口大小（字节数）
    uint16_t _window_size{1};

    // ！是否发送SYN
    bool _syn_sent{false};

    // ！是否发送FIN
    bool _fin_sent{false};

  public:
    //! Initialize a TCPSender
    TCPSender(const size_t capacity = TCPConfig::DEFAULT_CAPACITY,
              const uint16_t retx_timeout = TCPConfig::TIMEOUT_DFLT,
              const std::optional<WrappingInt32> fixed_isn = {});

    //! \name "Input" interface for the writer
    //!@{
    ByteStream &stream_in() { return _stream; }
    const ByteStream &stream_in() const { return _stream; }
    //!@}

    //! \name Methods that can cause the TCPSender to send a segment
    //!@{

    //! \brief A new acknowledgment was received
    void ack_received(const WrappingInt32 ackno, const uint16_t window_size);

    //! \brief Generate an empty-payload segment (useful for creating empty ACK segments)
    void send_empty_segment();

    //! \brief create and send segments to fill as much of the window as possible
    void fill_window();

    //! \brief Notifies the TCPSender of the passage of time
    void tick(const size_t ms_since_last_tick);
    //!@}

    //! \name Accessors
    //!@{

    //! \brief How many sequence numbers are occupied by segments sent but not yet acknowledged?
    //! \note count is in "sequence space," i.e. SYN and FIN each count for one byte
    //! (see TCPSegment::length_in_sequence_space())
    size_t bytes_in_flight() const;

    //! \brief Number of consecutive retransmissions that have occurred in a row
    unsigned int consecutive_retransmissions() const;

    //! \brief TCPSegments that the TCPSender has enqueued for transmission.
    //! \note These must be dequeued and sent by the TCPConnection,
    //! which will need to fill in the fields that are set by the TCPReceiver
    //! (ackno and window size) before sending.
    std::queue<TCPSegment> &segments_out() { return _segments_out; }
    //!@}

    //! \name What is the next sequence number? (used for testing)
    //!@{

    //! \brief absolute seqno for the next byte to be sent
    uint64_t next_seqno_absolute() const { return _next_seqno; }

    //! \brief relative seqno for the next byte to be sent
    WrappingInt32 next_seqno() const { return wrap(_next_seqno, _isn); }
    //!@}
};

#endif  // SPONGE_LIBSPONGE_TCP_SENDER_HH
