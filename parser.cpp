#include "parser.h"
#include <string>
#include <cstring>
#include "utils.h"
#include "keyUtils.h"
#include "logger.h"
#include "K12AndKeyUtil.h"
#define QU_TRANSFER 0
#define QU_TRANSFER_LOG_SIZE 72
#define ASSET_ISSUANCE 1
#define ASSET_ISSUANCE_LOG_SIZE 55
#define ASSET_OWNERSHIP_CHANGE 2
#define ASSET_OWNERSHIP_CHANGE_LOG_SIZE 119
#define ASSET_POSSESSION_CHANGE 3
#define ASSET_POSSESSION_CHANGE_LOG_SIZE 119
#define CONTRACT_ERROR_MESSAGE 4
#define CONTRACT_ERROR_MESSAGE_LOG_SIZE 4
#define CONTRACT_WARNING_MESSAGE 5
#define CONTRACT_INFORMATION_MESSAGE 6
#define CONTRACT_DEBUG_MESSAGE 7
#define CUSTOM_MESSAGE 255

#define LOG_HEADER_SIZE 26 // 2 bytes epoch + 4 bytes tick + 4 bytes log size/types + 8 bytes log id + 8 bytes log digest

std::string logTypeToString(uint8_t type){
    switch(type){
        case 0:
            return "QU transfer";
        case 1:
            return "Asset issuance";
        case 2:
            return "Asset ownership change";
        case 3:
            return "Asset possession change";
        case 4:
            return "Contract error";
        case 5:
            return "Contract warning";
        case 6:
            return "Contract info";
        case 7:
            return "Contract debug";
        case 255:
            return "Custom msg";
    }
    return "Unknown msg";
}
std::string parseLogToString_type0(uint8_t* ptr){
    char sourceIdentity[61] = {0};
    char destIdentity[61] = {0};;
    uint64_t amount;
    const bool isLowerCase = false;
    getIdentityFromPublicKey(ptr, sourceIdentity, isLowerCase);
    getIdentityFromPublicKey(ptr+32, destIdentity, isLowerCase);
    memcpy(&amount, ptr+64, 8);
    std::string result = "from " + std::string(sourceIdentity) + " to " + std::string(destIdentity) + " " + std::to_string(amount) + "QU.";
    return result;
}
std::string parseLogToString_type1(uint8_t* ptr){
    char sourceIdentity[61] = {0};
    char name[8] = {0};
    char numberOfDecimalPlaces = 0;
    uint8_t unit[8] = {0};

    long long numberOfShares = 0;
    const bool isLowerCase = false;
    getIdentityFromPublicKey(ptr, sourceIdentity, isLowerCase);
    memcpy(&numberOfShares, ptr+32, 8);
    memcpy(name, ptr+32+8, 7);
    numberOfDecimalPlaces = ((char*)ptr)[32+8+7];
    memcpy(unit, ptr+32+8+7+1, 7);
    std::string result = std::string(sourceIdentity) + " issued " + std::to_string(numberOfShares) + " " + std::string(name)
                       + ". Number of decimal: " + std::to_string(numberOfDecimalPlaces) + ". Unit of measurement: "
                       + std::to_string(unit[0]) + "-"
                       + std::to_string(unit[1]) + "-"
                       + std::to_string(unit[2]) + "-"
                       + std::to_string(unit[3]) + "-"
                       + std::to_string(unit[4]) + "-"
                       + std::to_string(unit[5]) + "-"
                       + std::to_string(unit[6]);
    return result;
}
std::string parseLogToString_qutil(uint8_t* ptr){
    std::string res = "";
    char buffer[64] = {0};
    getIdentityFromPublicKey(ptr, buffer, false);
    res = "from " + std::string(buffer) + " to ";
    getIdentityFromPublicKey(ptr+32, buffer, false);
    res += std::string(buffer) + " Amount ";
    int64_t amount;
    memcpy(&amount, ptr+64, 8);
    res += std::to_string(amount) + ": ";
    uint32_t logtype;
    memcpy(&logtype, ptr+72, 4);
    switch(logtype){
        case 0:
            res += "Success";
            break;
        case 1:
            res += "Invalid amount number";
            break;
        case 2:
            res += "insufficient fund";
            break;
        case 3:
            res += "Triggered SendToManyV1";
            break;
        case 4:
            res += "send fund via SendToManyV1";
            break;
    }
    return res;
}
std::string parseLogToString_type2_type3(uint8_t* ptr){
    char sourceIdentity[61] = {0};
    char dstIdentity[61] = {0};
    char issuerIdentity[61] = {0};
    char name[8] = {0};
    char numberOfDecimalPlaces = 0;
    char unit[8] = {0};
    long long numberOfShares = 0;
    const bool isLowerCase = false;
    getIdentityFromPublicKey(ptr, sourceIdentity, isLowerCase);
    getIdentityFromPublicKey(ptr+32, dstIdentity, isLowerCase);
    getIdentityFromPublicKey(ptr+64, issuerIdentity, isLowerCase);
    memcpy(&numberOfShares, ptr+96, 8);
    memcpy(name, ptr+96+8, 7);
    numberOfDecimalPlaces = ((char*)ptr)[96+8+7];
    memcpy(unit, ptr+96+8+7+1, 7);
    std::string result = "from " + std::string(sourceIdentity) + " to " + std::string(dstIdentity) + " " + std::to_string(numberOfShares) + " " + std::string(name)
                         + "(Issuer: " + std::string(issuerIdentity) + ")"
                         + ". Number of decimal: " + std::to_string(numberOfDecimalPlaces) + ". Unit of measurement: "
                         + std::to_string(unit[0]) + "-"
                         + std::to_string(unit[1]) + "-"
                         + std::to_string(unit[2]) + "-"
                         + std::to_string(unit[3]) + "-"
                         + std::to_string(unit[4]) + "-"
                         + std::to_string(unit[5]) + "-"
                         + std::to_string(unit[6]);
    return result;
}
unsigned long long printQubicLog(uint8_t* logBuffer, int bufferSize){
    if (bufferSize == 0){
//        LOG("Empty log\n");
        return -1;
    }
    if (bufferSize < LOG_HEADER_SIZE){
        LOG("Buffer size is too small (not enough to contain the header), expected 16 | received %d\n", bufferSize);
        return -1;
    }
    uint8_t* end = logBuffer + bufferSize;
    unsigned long long retLogId = 0;
    while (logBuffer < end){
        // basic info
        uint16_t epoch = *((unsigned char*)(logBuffer));
        uint32_t tick = *((unsigned int*)(logBuffer + 2));
        uint32_t tmp = *((unsigned int*)(logBuffer + 6));
        uint64_t logId = *((unsigned long long*)(logBuffer + 10));
        if (logId > retLogId) retLogId = logId;
        uint64_t logDigest = *((unsigned long long*)(logBuffer + 18));
        uint8_t messageType = tmp >> 24;
        std::string mt = logTypeToString(messageType);
        uint32_t messageSize = (tmp << 8) >> 8;
        {
            uint64_t computedLogDigest = 0;
            KangarooTwelve(logBuffer + LOG_HEADER_SIZE, messageSize, (uint8_t*) & computedLogDigest, 8);
            if (logDigest != computedLogDigest)
            {
                LOG("WARNING: mismatched log digest\n");
            }
        }
        logBuffer += LOG_HEADER_SIZE;
        std::string humanLog = "null";
        switch(messageType){
            case QU_TRANSFER:
                if (messageSize == QU_TRANSFER_LOG_SIZE){ // with or without transfer ID
                    humanLog = parseLogToString_type0(logBuffer);
                } else {
                    LOG("Malfunction buffer size for QU_TRANSFER log\n");
                }
                break;
            case ASSET_ISSUANCE:
                if (messageSize == ASSET_ISSUANCE_LOG_SIZE){
                    humanLog = parseLogToString_type1(logBuffer);
                } else {
                    LOG("Malfunction buffer size for ASSET_ISSUANCE log\n");
                }
                break;
            case ASSET_OWNERSHIP_CHANGE:
                if (messageSize == ASSET_OWNERSHIP_CHANGE_LOG_SIZE){
                    humanLog = parseLogToString_type2_type3(logBuffer);
                } else {
                    LOG("Malfunction buffer size for ASSET_OWNERSHIP_CHANGE log\n");
                }
                break;
            case ASSET_POSSESSION_CHANGE:
                if (messageSize == ASSET_POSSESSION_CHANGE_LOG_SIZE){
                    humanLog = parseLogToString_type2_type3(logBuffer);
                } else {
                    LOG("Malfunction buffer size for ASSET_POSSESSION_CHANGE log\n");
                }
                break;
            // TODO: stay up-to-date with core node contract logger
            case CONTRACT_INFORMATION_MESSAGE:
            case CONTRACT_ERROR_MESSAGE:
            case CONTRACT_WARNING_MESSAGE:
            case CONTRACT_DEBUG_MESSAGE:
            case 255:
            {
                unsigned int contractId = ((uint32_t*)logBuffer)[0];
                humanLog = "Contract ID #" + std::to_string(contractId) + " ";
                if (messageType == CONTRACT_INFORMATION_MESSAGE) humanLog += "INFO: ";
                if (messageType == CONTRACT_ERROR_MESSAGE) humanLog += "ERROR: ";
                if (messageType == CONTRACT_WARNING_MESSAGE) humanLog += "WARNING: ";
                if (messageType == CONTRACT_DEBUG_MESSAGE) humanLog += "DEBUG: ";
                if (messageType == 255) humanLog += "CUSTOM: ";
                char buff[1024 * 2] = { 0 };
                byteToHex(logBuffer + 4, buff, messageSize - 4);
                humanLog += std::string(buff);
                break;
            }
        }
        LOG("[%llu] %u.%03d %s: %s\n", logId, tick, epoch, mt.c_str(), humanLog.c_str());
        if (humanLog == "null"){
            char buff[1024*2] = {0};
            for (unsigned int i = 0; i < messageSize; i++){
                sprintf(buff + i*2, "%02x", logBuffer[i]);
            }
            LOG("NO parser for this message yet | Original message: %s\n", buff);
        }
        logBuffer+= messageSize;
    }
    return retLogId;
}