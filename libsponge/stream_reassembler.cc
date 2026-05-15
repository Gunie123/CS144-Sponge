#include "stream_reassembler.hh"

// Dummy implementation of a stream reassembler.

// For Lab 1, please replace with a real implementation that passes the
// automated checks run by `make check_lab1`.

// You will need to add private members to the class declaration in `stream_reassembler.hh`

template <typename... Targs>
void DUMMY_CODE(Targs &&.../* unused */) {}

using namespace std;

StreamReassembler::StreamReassembler(const size_t capacity)
    : _output(capacity)
    , _capacity(capacity)
    , _buf(capacity)
    , _valid(capacity, false)
    , _cur_index(0)
    , _eof_idx(std::numeric_limits<size_t>::max())
    , _unassembled_bytes_cnt(0) {}

//! \details This function accepts a substring (aka a segment) of bytes,
//! possibly out-of-order, from the logical stream, and assembles any newly
//! contiguous substrings and writes them into the output stream in order.
void StreamReassembler::push_substring(const string &data, const size_t index, const bool eof) {
    auto st = max(index, _cur_index);
    auto cap_right = _cur_index + _capacity - _output.buffer_size();
    auto ed = min(index + data.size(), min(cap_right, _eof_idx));
    if (eof)
        _eof_idx = min(_eof_idx, index + data.size());
    for (size_t i = st, j = st - index; i < ed; ++i, ++j) {
        size_t slot = i % _capacity;
        if (!_valid[slot]) {
            _buf[slot] = data[j];
            _valid[slot] = true;
            ++_unassembled_bytes_cnt;
        }
    }
    string str;
    while (_cur_index < _eof_idx && _valid[_cur_index % _capacity]) {
        size_t slot = _cur_index % _capacity;
        str.push_back(_buf[slot]);
        _valid[slot] = false;
        ++_cur_index;
        --_unassembled_bytes_cnt;
    }
    _output.write(str);

    if (_cur_index == _eof_idx)
        _output.end_input();
}

size_t StreamReassembler::unassembled_bytes() const { return _unassembled_bytes_cnt; }

bool StreamReassembler::empty() const { return _unassembled_bytes_cnt == 0; }
