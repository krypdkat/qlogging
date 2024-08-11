#include <thread>
#include <chrono>
#include "stdio.h"
#include "connection.h"
#include "parser.h"
#include "structs.h"

template <typename T>
T charToNumber(char* a)
{
    T retVal = 0;
    char *endptr = nullptr;
    retVal = strtoull(a, &endptr, 10);
    return retVal;
}

void getTickData(QCPtr& qc, const uint32_t tick, TickData& result)
{
    memset(&result, 0, sizeof(TickData));
    static struct
    {
        RequestResponseHeader header;
        RequestTickData requestTickData;
    } packet;
    packet.header.setSize(sizeof(packet));
    packet.header.randomizeDejavu();
    packet.header.setType(REQUEST_TICK_DATA);
    packet.requestTickData.requestedTickData.tick = tick;
    int sent = qc->sendData((uint8_t *) &packet, packet.header.size());
    result = qc->receivePacketAs<TickData>();
    return;
}

void getLogFromNode(QCPtr& qc, uint64_t* passcode, uint64_t fromId, uint64_t toId)
{
    struct {
        RequestResponseHeader header;
        unsigned long long passcode[4];
        unsigned long long fromid;
        unsigned long long toid;
    } packet;
    packet.header.setSize(sizeof(packet));
    packet.header.randomizeDejavu();
    packet.header.setType(RequestLog::type());
    memcpy(packet.passcode, passcode, 4 * sizeof(uint64_t));
    packet.fromid = fromId;
    packet.toid = toId;
    qc->sendData((uint8_t *) &packet, packet.header.size());
    std::vector<uint8_t> buffer;
    qc->receiveDataAll(buffer);
    uint8_t* data = buffer.data();
    int recvByte = buffer.size();
    int ptr = 0;
    while (ptr < recvByte)
    {
        auto header = (RequestResponseHeader*)(data+ptr);
        if (header->type() == RespondLog::type()){
            auto logBuffer = (uint8_t*)(data + ptr + sizeof(RequestResponseHeader));
            printQubicLog(logBuffer, header->size() - sizeof(RequestResponseHeader));
        }
        ptr+= header->size();
    }
}

void getLogIdRange(QCPtr& qc, uint64_t* passcode, uint32_t requestedTick, uint32_t txsId, long long& fromId, long long& toId)
{
    struct {
        RequestResponseHeader header;
        unsigned long long passcode[4];
        unsigned int tick;
        unsigned int txId;
    } packet;
    packet.header.setSize(sizeof(packet));
    packet.header.randomizeDejavu();
    packet.header.setType(RequestLogIdRange::type());
    memcpy(packet.passcode, passcode, 4 * sizeof(uint64_t));
    packet.tick = requestedTick;
    packet.txId = txsId;
    qc->sendData((uint8_t *) &packet, packet.header.size());
    auto result = qc->receivePacketAs<ResponseLogIdRange>();
    fromId = result.fromLogId;
    toId = fromId + result.length - 1;
}

static CurrentTickInfo getTickInfoFromNode(QCPtr& qc)
{
    CurrentTickInfo result;
    memset(&result, 0, sizeof(CurrentTickInfo));
    struct {
        RequestResponseHeader header;
    } packet;
    packet.header.setSize(sizeof(packet));
    packet.header.randomizeDejavu();
    packet.header.setType(REQUEST_CURRENT_TICK_INFO);
    qc->sendData((uint8_t *) &packet, packet.header.size());
    std::vector<uint8_t> buffer;
    qc->receiveDataAll(buffer);
    uint8_t* data = buffer.data();
    int recvByte = buffer.size();
    int ptr = 0;
    while (ptr < recvByte)
    {
        auto header = (RequestResponseHeader*)(data+ptr);
        if (header->type() == RESPOND_CURRENT_TICK_INFO){
            auto curTickInfo = (CurrentTickInfo*)(data + ptr + sizeof(RequestResponseHeader));
            result = *curTickInfo;
        }
        ptr+= header->size();
    }
    return result;
}
uint32_t getTickNumberFromNode(QCPtr& qc)
{
    auto curTickInfo = getTickInfoFromNode(qc);
    return curTickInfo.tick;
}

int run(int argc, char* argv[])
{
    if (argc != 8)
    {
        printf("./qlogging [nodeip] [nodeport] [passcode u64 x 4] [tick to start]\n");
        return 0;
    }
    char* nodeIp = argv[1];
    int nodePort = charToNumber<int>(argv[2]);
    uint64_t passcode[4] = {charToNumber<unsigned long long>(argv[3]), charToNumber<unsigned long long>(argv[4]),
                            charToNumber<unsigned long long>(argv[5]), charToNumber<unsigned long long>(argv[6])};
    unsigned int tick = charToNumber<unsigned int>(argv[7]);
    QCPtr qc;
    uint32_t currentTick = 0;
    bool needReconnect = true;
    while (1)
    {
        try {
            if (needReconnect)
            {
                qc = make_qc(nodeIp, nodePort);
                // do the handshake stuff
                std::vector<uint8_t> buff;
                qc->receiveDataAll(buff);
                needReconnect = false;
            }
            if (currentTick == 0 || currentTick <= tick) {
                currentTick = getTickNumberFromNode(qc);
            }
            if (currentTick <= tick) {
                printf("Current tick %u vs local tick %u | sleep 3s\n", currentTick, tick);
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
            TickData td;
            memset(&td, 0, sizeof(td));
            getTickData(qc, tick, td);
            if (isArrayZero((uint8_t *) &td, sizeof(td))) {
                printf("Tick %u is empty\n", tick);
                fflush(stdout);
                tick++;
                continue;
            }
            for (int i = 0; i < 1024; i++) {
                if (isArrayZero(td.transactionDigests[i], 32)) break;
                long long fromId = 0, toId = 0;
                getLogIdRange(qc, passcode, tick, i, fromId, toId);
                if (fromId != -1 && toId != -1) {
                    printf("Tick %u Transaction #%d has log from %lld to %lld. Trying to fetch...\n", tick, i, fromId,
                           toId);
                    getLogFromNode(qc, passcode, fromId, toId);
                } else {
                    printf("Tick %u Transaction #%d doesn't generate any log\n", tick, i);
                }
            }
            tick++;
            fflush(stdout);
        }
        catch (std::logic_error & ex)
        {
            printf("%s\n", ex.what());
            fflush(stdout);
            needReconnect = true;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

int main(int argc, char* argv[])
{
    try
    {
        return run(argc, argv);
    }
    catch (std::exception & ex)
    {
        printf("%s\n", ex.what());
        return -1;
    }
}
