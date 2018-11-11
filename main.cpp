#include <algorithm>
#include <array>
#include <cctype>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

#include <netinet/in.h>

#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#define EVER ;;

using namespace std;

constexpr uint32_t microsecondsInSecond = 1000000;

class EventStatistic
{
    uint32_t _minimum;
    uint32_t _maximum;
    uint32_t _median;
    uint32_t _122us_90_0_percentel;
    uint32_t _140us_99_0_percentel;
    uint32_t _145us_99_9_percentel;

    uint32_t _total_counter;
    uint32_t _below_122us_counter;
    uint32_t _below_140us_counter;
    uint32_t _below_145us_counter;

    array<uint32_t, microsecondsInSecond> _histogram;
public:
    EventStatistic()
    {
        Reset();
    }

    void Reset()
    {
        _minimum = 0;
        _maximum = 0;
        _median = 0;
        _122us_90_0_percentel = 0;
        _140us_99_0_percentel = 0;
        _145us_99_9_percentel = 0;
        _total_counter = 0;
        _below_122us_counter = 0;
        _below_140us_counter = 0;
        _below_145us_counter = 0;
        fill(_histogram.begin(), _histogram.end(), 0);
    }

    void AddData(uint32_t delay)
    {
        delay %= microsecondsInSecond;
        _histogram[delay]++;
        _total_counter++;

        if (delay < _minimum || _minimum == 0)
            _minimum = delay;

        if (delay > _maximum)
            _maximum = delay;

        if (delay < 122)
            _below_122us_counter++;

        if (delay < 140)
            _below_140us_counter++;

        if (delay < 145)
            _below_145us_counter++;
    }

    void Calc()
    {
        float local_median_counter = 0;
        float local_122us_counter = 0;
        float local_140us_counter = 0;
        float local_145us_counter = 0;

        for (uint32_t delay = 0; delay < microsecondsInSecond; delay++)
        {
            uint32_t count = _histogram[delay];
            if (count == 0)
                continue;

            if (local_median_counter / _total_counter < 50.0)
            {
                _median = delay;
                local_median_counter += count;
            }

            if (delay < 122)
            {
                if (local_122us_counter / _below_122us_counter < 90.0)
                {
                    _122us_90_0_percentel = delay;
                    local_122us_counter += count;
                }
            }

            if (delay < 140)
            {
                if (local_140us_counter / _below_140us_counter < 99.0)
                {
                    _140us_99_0_percentel = delay;
                    local_140us_counter += count;
                }
            }

            if (delay < 145)
            {
                if (local_145us_counter / _below_145us_counter < 99.9)
                {
                    _145us_99_9_percentel = delay;
                    local_145us_counter += count;
                }
            }
        }
    }

    uint32_t GetMinimun() const { return _minimum; }
    uint32_t GetMedian() const { return _median; }
    uint32_t Get122us90Percentel() const { return _122us_90_0_percentel; }
    uint32_t Get140us99Percentel() const { return _140us_99_0_percentel; }
    uint32_t Get145us999Percentel() const { return _145us_99_9_percentel; }

    string GetReport()
    {
        stringstream result;
        result << "min=" << GetMinimun()
               << " 50%=" << GetMedian()
               << " 90%=" << Get122us90Percentel()
               << " 99%=" << Get140us99Percentel()
               << " 99.9%=" << Get145us999Percentel();

        return result.str();
    }

    string GetFullReport()
    {
        uint32_t step = 5;
        uint32_t range_min = _minimum - _minimum % step;
        uint32_t range_max = _maximum - _maximum % step + step;

        stringstream buf;
        buf << "ExecTime\tTransNo\tWeight,%\tPercent" << endl;
        uint32_t trans_no_below_exec_time = 0;
        for (uint32_t exec_time = range_min; exec_time < range_max; exec_time += step)
        {
            uint32_t trans_no = 0;
            for (uint32_t i = exec_time; i < exec_time + step; i++)
            {
                trans_no += _histogram[i];
            }

            if (trans_no == 0)
                continue;

            float weight = (float)(trans_no) / _total_counter * 100.0;
            float percent = (float)(trans_no_below_exec_time) / _total_counter * 100.0;
            trans_no_below_exec_time += trans_no;

            buf << exec_time << "\t" << trans_no << "\t" << weight << "\t" << percent << endl;
        }

        return buf.str();
    }
};

class Statistic
{
    using Event = string;
    map<Event, EventStatistic> _events;
    recursive_mutex _mutex;
public:
    void AddEvent(Event event)
    {
        lock_guard<recursive_mutex> guard(_mutex);
        if (_events.count(event) == 0)
            _events.insert(make_pair(event, EventStatistic()));
    }

    void AddEventData(Event event, uint32_t delay)
    {
        AddEvent(event);
        lock_guard<recursive_mutex> guard(_mutex);
        _events[event].AddData(delay);
    }

    string GetEventStatistic(Event event)
    {
        AddEvent(event);
        stringstream result;
        result << event << " ";
        lock_guard<recursive_mutex> guard(_mutex);
        _events[event].Calc();
        result << _events[event].GetReport() << endl;

        return result.str();
    }

    string GetFullStatistic()
    {
        lock_guard<recursive_mutex> guard(_mutex);
        stringstream result;
        for (auto& e:_events)
        {
            e.second.Calc();
            result << e.first << " " << e.second.GetReport() << endl;
            result << e.second.GetFullReport() << endl;
        }

        return result.str();
    }
};

class TCPServer
{
    int port = 12345;
    int sock = -1;
    int backlog = 100000;
public:
    TCPServer()
    {
        cout << "TCP Server constructor" << endl;
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0)
            throw logic_error(string("socket() failed: ") + strerror(errno));

        struct sockaddr_in serv_addr = {};
        memset((char*) &serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family      = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port        = htons(port);

        if (bind(sock, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0)
            throw logic_error(string("bind() failed: ") + strerror(errno));

        if (listen(sock, backlog) < 0)
            throw logic_error(string("listen() failed: ") + strerror(errno));
    }

    ~TCPServer()
    {
        cout << "TCP Server destructor" << endl;
        shutdown(sock, SHUT_RDWR);
        close(sock);
    }

    int GetDescriptor() const { return sock; }

    int AcceptNewConnection()
    {
        struct sockaddr_in cli_addr;
        socklen_t sock_len = sizeof(cli_addr);
        int newsock = accept(sock, (struct sockaddr *) &cli_addr, &sock_len);
        if (newsock < 0)
            throw logic_error(string("accept() failed: ") + strerror(errno));

        return newsock;
    }
};

class UDPServer
{
    int port = 12346;
    int sock = -1;
public:
    UDPServer()
    {
        cout << "UDP Server constructor" << endl;
        sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock < 0)
            throw logic_error(string("socket() failed: ") + strerror(errno));

        struct sockaddr_in serv_addr = {};
        memset((char*) &serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family      = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port        = htons(port);

        if (bind(sock, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0)
            throw logic_error(string("bind() failed: ") + strerror(errno));
    }

    ~UDPServer()
    {
        cout << "UDP Server destructor" << endl;
        shutdown(sock, SHUT_RDWR);
        close(sock);
    }

    int GetDescriptor() const { return sock; }
};

class InputFile
{
    int descriptor;
    string name = "input_file.txt";
public:
    InputFile()
    {
        cout << "Input file constructor" << endl;
        descriptor = open(name.c_str(), O_CREAT, S_IRUSR | S_IWUSR | O_RDONLY);
        if (descriptor < 0)
            throw logic_error(string("open() failed: ") + strerror(errno));
    }

    ~InputFile()
    {
        cout << "Input file destructor" << endl;
        close(descriptor);
    }

    int GetDescriptor() const { return descriptor; }
};

class InputFIFO
{
    int descriptor;
    string name = "input_fifo.txt";
public:
    InputFIFO()
    {
        cout << "FIFO constructor" << endl;
        unlink(name.c_str());
        if (mkfifo(name.c_str(), S_IRUSR | S_IWUSR) < 0)
            throw logic_error(string("mkfifo() failed: ") + strerror(errno));

        descriptor = open(name.c_str(), O_RDONLY | O_NONBLOCK);
        if (descriptor < 0)
            throw logic_error(string("open() failed: ") + strerror(errno));
    }

    ~InputFIFO()
    {
        cout << "FIFO destructor" << endl;
        close(descriptor);
    }

    int GetDescriptor() const { return descriptor; }
};

void MainHandler(int descriptor, Statistic& stat)
{
    string event;
    string avgtsmr;
    uint32_t delay;

    int section = 0;
    int tab_counter = 0;

    size_t cur_size = 0;
    constexpr size_t max_size = 100000;
    char buf[max_size];

    for (EVER)
    {
        cur_size = read(descriptor, buf, max_size);
        if (cur_size == 0)
            break;

        for (size_t i = 0; i < cur_size; i++)
        {
            if (section == 0)       // waiting the end char of the TIME column
            {
                if (buf[i] == ']')
                    section++;
            }
            else if (section == 1)  // skiping '\t' char
            {
                if (buf[i] == '\t')
                {
                    section++;
                    event = "";
                }
                else
                    section = 0;
            }
            else if (section == 2)  // reading EVENT field
            {
                if (buf[i] == '\t')
                {
                    tab_counter = 1;
                    section++;
                }
                else
                    event += buf[i];
            }
            else if (section == 3)  // skipping all fields in the middle
            {
                if (buf[i] == '\t')
                    tab_counter++;

                if (tab_counter == 14)
                {
                    section++;
                    avgtsmr = "";
                }
            }
            else if (section == 4)  // reading AVGTSMR field
            {
                if (isdigit(buf[i]))
                    avgtsmr += buf[i];
                else
                {
                    delay = stoi(avgtsmr);
                    stat.AddEventData(event, delay);
                    section = 0;
                }
            }
        }
    }
}

void TCPHandler(int descriptor, Statistic& stat)
{
    MainHandler(descriptor, stat);
    close(descriptor);
}

void UDPHandler(int descriptor, Statistic& stat)
{
    size_t len = 1024;
    char buf[len];
    struct sockaddr src_addr;
    socklen_t addrlen = sizeof(src_addr);

    recvfrom(descriptor, buf, len, 0, &src_addr, &addrlen);

    string event(buf);
    string report = stat.GetEventStatistic(event);
    sendto(descriptor, report.c_str(), report.size(), 0, &src_addr, addrlen);
}

static Statistic stats;
volatile bool isWorking = true;

void SIGINT_Handler(int signo)
{
    cout << "SIGINT Handler" << endl;
    isWorking = false;
}

void SIGUSR1_Handler(int signo)
{
    cout << "SIGUSR1 Handler" << endl;
    cout << stats.GetFullStatistic();
}

void SIGUSR2_Handler(int signo)
{
    cout << "SIGUSR2 Handler" << endl;
    cout << stats.GetFullStatistic();
}

int main()
{
    try
    {
        if (signal(SIGINT, SIGINT_Handler) == SIG_ERR)
            throw logic_error(string("signal() (SIGINT) failed: ") + strerror(errno));

        if (signal(SIGUSR1, SIGUSR1_Handler) == SIG_ERR)
            throw logic_error(string("signal() (SIGUSR1) failed: ") + strerror(errno));

        if (signal(SIGUSR2, SIGUSR2_Handler) == SIG_ERR)
            throw logic_error(string("signal() (SIGUSR2) failed: ") + strerror(errno));

        InputFile inputFile;
        InputFIFO inputFIFO;
        TCPServer tcpServer;
        UDPServer udpServer;

        /*
            We calc stats on regular file here because
            of the problem described below.
        */
        MainHandler(inputFile.GetDescriptor(), stats);

        int nfds = 3;
        struct pollfd fds[nfds] = {
            //{ .fd = inputFile.GetDescriptor(), .events = POLLIN },
            { .fd = inputFIFO.GetDescriptor(), .events = POLLIN },
            { .fd = tcpServer.GetDescriptor(), .events = POLLIN },
            { .fd = udpServer.GetDescriptor(), .events = POLLIN }
        };

        int mstimeout = 3 * 1000;

        while (isWorking)
        {
            cout << "polling..." << endl;
            int rc = poll(fds, nfds, mstimeout);
            if (rc < 0)
            {
                throw logic_error(string("bind() failed: ") + strerror(errno));
            }
            else if (rc == 0)
            {
                cout << "poll() timed out" << endl;
                continue;
            }

            cout << "searchin readable descriptor..." << endl;
            for (int i = 0; i < nfds; i++)
            {
                if (fds[i].revents != POLLIN)
                    continue;

                int cur_fd = fds[i].fd;
                /*
                    Problem with polling regular file.
                    Regular file is alwase ready for reading.
                    Beacuse of this server starts run this code
                    like while(1). Because of this i move code
                    for reading regular file upper to server
                    initialisation.

                if (cur_fd == inputFile.GetDescriptor())
                {
                    MainHandler(cur_fd, stats);
                }
                else
                */
                if (cur_fd == inputFIFO.GetDescriptor())
                {
                    MainHandler(cur_fd, stats);
                }
                else if (cur_fd == udpServer.GetDescriptor())
                {
                    UDPHandler(cur_fd, stats);
                }
                else if (cur_fd == tcpServer.GetDescriptor())
                {
                    int newConnDescriptor = tcpServer.AcceptNewConnection();
                    thread (TCPHandler, newConnDescriptor, ref(stats)).detach();
                }
            }
        }
    }
    catch(exception& e)
    {
        cerr << e.what();
    }

    return 0;
}
