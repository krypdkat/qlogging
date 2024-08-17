#include <thread>
#include <chrono>
#include "stdio.h"
#include "connection.h"
#include "parser.h"
#include "structs.h"
#include "keyUtils.h"
#include "K12AndKeyUtil.h"
#include "logger.h"
#include <stdexcept>
#undef ARBITRATOR
#define ARBITRATOR "MEFKYFCDXDUILCAJKOIKWQAPENJDUHSSYPBRWFOTLALILAYWQFDSITJELLHG"

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
    memset(&packet, 0, sizeof(packet));
    packet.header.setSize(sizeof(packet));
    packet.header.randomizeDejavu();
    packet.header.setType(RequestLog::type());
    memcpy(packet.passcode, passcode, 4 * sizeof(uint64_t));
    packet.fromid = fromId;
    packet.toid = toId;
    qc->sendData((uint8_t *) &packet, packet.header.size());
    std::vector<uint8_t> buffer;
    qc->receiveAFullPacket(buffer);
    uint8_t* data = buffer.data();
    int recvByte = buffer.size();
    int ptr = 0;
    unsigned long long retLogId = -1; // max uint64
    while (ptr < recvByte)
    {
        auto header = (RequestResponseHeader*)(data+ptr);
        if (header->type() == RespondLog::type()){
            auto logBuffer = (uint8_t*)(data + ptr + sizeof(RequestResponseHeader));
            retLogId = printQubicLog(logBuffer, header->size() - sizeof(RequestResponseHeader));
        }
        ptr+= header->size();
    }

    if (retLogId < toId)
    {
        // round buffer case, only the first half returned, call one more time to print out another half
        getLogFromNode(qc, passcode, retLogId + 1, toId);
    }
    if (retLogId == -1)
    {
        printf("WARNING: Unexpected value for retLogId\n");
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
    memset(&packet, 0, sizeof(packet));
    packet.header.setSize(sizeof(packet));
    packet.header.randomizeDejavu();
    packet.header.setType(RequestLogIdRange::type());
    memcpy(packet.passcode, passcode, 4 * sizeof(uint64_t));
    packet.tick = requestedTick;
    packet.txId = txsId;
    qc->sendData((uint8_t *) &packet, packet.header.size());
    auto result = qc->receivePacketAs<ResponseLogIdRange>();
    if (result.length)
    {
        fromId = result.fromLogId;
        toId = fromId + result.length - 1;
    }
    else
    {
        fromId = -1;
        toId = -1;
    }

}

static CurrentTickInfo getTickInfoFromNode(QCPtr& qc)
{
    CurrentTickInfo result;
    memset(&result, 0, sizeof(CurrentTickInfo));
    struct {
        RequestResponseHeader header;
    } packet;
    memset(&packet, 0, sizeof(packet));
    packet.header.setSize(sizeof(packet));
    packet.header.randomizeDejavu();
    packet.header.setType(REQUEST_CURRENT_TICK_INFO);
    qc->sendData((uint8_t *) &packet, packet.header.size());
    std::vector<uint8_t> buffer;
    qc->receiveAFullPacket(buffer);
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

static void getTickTransactions(QCPtr& qc, const uint32_t requestedTick, int requestedTxId,
                                Transaction& txs, //out
                                extraDataStruct& extraData // out
)
{
    struct {
        RequestResponseHeader header;
        RequestedTickTransactions txs;
    } packet;
    memset(&packet, 0, sizeof(packet));
    packet.header.setSize(sizeof(packet));
    packet.header.randomizeDejavu();
    packet.header.setType(REQUEST_TICK_TRANSACTIONS); // REQUEST_TICK_TRANSACTIONS
    packet.txs.tick = requestedTick;
    for (int i = 0; i < (1024+7)/8; i++) packet.txs.transactionFlags[i] = 0xff;

    {
        packet.txs.transactionFlags[requestedTxId>>3] &= ~(1 << (requestedTxId & 7));
    }
    qc->sendData((uint8_t *) &packet, packet.header.size());

    {
        std::vector<uint8_t> buffer;
        qc->receiveAFullPacket(buffer);
        uint8_t* data = buffer.data();
        int recvByte = buffer.size();
        int ptr = 0;
        while (ptr < recvByte)
        {
            auto header = (RequestResponseHeader*)(data+ptr);
            if (header->type() == BROADCAST_TRANSACTION){
                auto tx = (Transaction *)(data + ptr + sizeof(RequestResponseHeader));
                txs = *tx;
                extraDataStruct ed;
                ed.vecU8.resize(tx->inputSize);
                if (tx->inputSize != 0){
                    memcpy(ed.vecU8.data(), reinterpret_cast<const uint8_t *>(tx) + sizeof(Transaction), tx->inputSize);
                }
                extraData = ed;
            }
            ptr+= header->size();
        }
    }
    {
        // receive END OF transmission
        std::vector<uint8_t> buffer;
        qc->receiveAFullPacket(buffer);
    }
}
void printReceipt(Transaction& tx, const char* txHash = nullptr, const uint8_t* extraData = nullptr, int moneyFlew = -1)
{
    char sourceIdentity[128] = {0};
    char dstIdentity[128] = {0};
    char txHashClean[128] = {0};
    bool isLowerCase = false;
    getIdentityFromPublicKey(tx.sourcePublicKey, sourceIdentity, isLowerCase);
    getIdentityFromPublicKey(tx.destinationPublicKey, dstIdentity, isLowerCase);
    LOG("~~~~~RECEIPT~~~~~\n");
    if (txHash != nullptr) {
        memcpy(txHashClean, txHash, 60);
        LOG("TxHash: %s\n", txHashClean);
    }
    LOG("From: %s\n", sourceIdentity);
    LOG("To: %s\n", dstIdentity);
    LOG("Input type: %u\n", tx.inputType);
    LOG("Amount: %lu\n", tx.amount);
    LOG("Tick: %u\n", tx.tick);
    LOG("Extra data size: %u\n", tx.inputSize);
    if (extraData != nullptr && tx.inputSize){
        char hex_tring[1024*2] = {0};
        for (int i = 0; i < tx.inputSize; i++)
            sprintf(hex_tring + i * 2, "%02x", extraData[i]);

        LOG("Extra data: %s\n", hex_tring);
    }
    if (moneyFlew != -1){
        if (moneyFlew) LOG("MoneyFlew: Yes\n");
        else LOG("MoneyFlew: No\n");
    } else {
        LOG("MoneyFlew: N/A\n");
    }
    LOG("~~~~~END-RECEIPT~~~~~\n");
}

uint32_t getTickNumberFromNode(QCPtr& qc)
{
    auto curTickInfo = getTickInfoFromNode(qc);
    return curTickInfo.tick;
}
TickData td;
int run(int argc, char* argv[])
{
    if (argc != 8)
    {
        printf("./qlogging [nodeip] [nodeport] [passcode u64 x 4] [tick to start]\n");
        return 0;
    }
    uint8_t arbPubkey[32];
    getPublicKeyFromIdentity(ARBITRATOR, arbPubkey);
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
                qc->receiveAFullPacket(buff);
                needReconnect = false;
            }
            if (currentTick == 0 || currentTick <= tick) {
                currentTick = getTickNumberFromNode(qc);
            }
            if (currentTick <= tick) {
                printf("Current tick %u vs local tick %u | sleep 3s\n", currentTick, tick);
                std::this_thread::sleep_for(std::chrono::seconds(3));
                continue;
            }
            
            memset(&td, 0, sizeof(td));
            getTickData(qc, tick, td);
            if (isArrayZero((uint8_t *) &td, sizeof(td))) {
                printf("Tick %u is empty\n", tick);
                fflush(stdout);
                tick++;
                continue;
            }

            // for debugging purpose, this one isn't needed in real code
            int nTx = 0;
            for (int i = 0; i < 1024; i++) {
                if (isArrayZero(td.transactionDigests[i], 32)) break;
                nTx++;
            }

            if (nTx)
            {
                for (int i = 0; i < nTx; i++) {
                    long long fromId = 0, toId = 0;
                    getLogIdRange(qc, passcode, tick, i, fromId, toId);
                    if (fromId < 0 || toId < 0) {
                        printf("Tick %u Transaction #%d doesn't generate any log - returned value %lld %lld\n", tick, i, fromId, toId);
                        // for debugging
                        Transaction txs;
                        extraDataStruct extraData;
                        getTickTransactions(qc, tick, i, txs, extraData);
                        if ( memcmp(txs.destinationPublicKey, arbPubkey, 32) == 0)
                        {
                            if (txs.inputSize == 32 && txs.amount == 0)
                            {
                                printf(">>> Reason: Solution txs has no log\n");
                            }
                            else
                            {
                                printf(">>> Reason: Invalid solution submission\n");
                            }
                        }
                        else if (isArrayZero(txs.destinationPublicKey, 32) && txs.inputSize == 848) // vote counter
                        {
                            printf(">>> Reason: Vote counter tx doesn't generate any log\n");
                        }
                        else if ( ((uint64_t*)txs.destinationPublicKey)[0] == 1) // QX
                        {
                            if (txs.inputType == 0)
                            {
                                printf(">>> Reason: Invalid QX invocation - InputType = 0\n");
                            }
                        }
                        else
                        {
                            printf(">>> Reason: Unknown. Tx detail:\n");
                            printReceipt(txs, nullptr, extraData.vecU8.data(), -1);
                        }
                        // end - for debugging
                    } else {
                        printf("Tick %u Transaction #%d has log from %lld to %lld. Trying to fetch...\n", tick, i, fromId,
                               toId);
                        getLogFromNode(qc, passcode, fromId, toId);
                    }
                }
            }
            else {
                printf("Tick %u has no transaction\n", tick);
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
