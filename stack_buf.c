// th_buffer_csv.c
// FIFO(/tmp/th_fifo)에서 JSON line을 읽어 CSV 파일(th_buffer.csv)에 장기 보관용으로 append 저장

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

#define FIFO_PATH "/tmp/th_fifo"
#define OUT_CSV   "th_buffer.csv"

static FILE* open_fifo_blocking(void) {
    // writer가 붙을 때까지 블록될 수 있음(정상 동작)
    return fopen(FIFO_PATH, "r");
}

// 아주 단순 JSON field 추출(외부 라이브러리 없이)
static int extract_float_field(const char* line, const char* key, float* outVal) {
    const char* kpos = strstr(line, key);
    if (!kpos) return 0;
    const char* cpos = strchr(kpos, ':');
    if (!cpos) return 0;
    cpos++; // ':' 다음
    while (*cpos == ' ' || *cpos == '\t') cpos++;
    *outVal = strtof(cpos, NULL);
    return 1;
}

// "ts":"2026-01-12T..." 같은 문자열 필드 추출
static int extract_string_field(const char* line, const char* key, char* out, size_t outSize) {
    const char* kpos = strstr(line, key);
    if (!kpos) return 0;

    // key 다음 첫 따옴표를 찾아 value 시작으로
    const char* q1 = strchr(kpos, ':');
    if (!q1) return 0;
    q1++;
    while (*q1 == ' ' || *q1 == '\t') q1++;
    if (*q1 != '\"') return 0;
    q1++; // value 시작

    const char* q2 = strchr(q1, '\"');
    if (!q2) return 0;

    size_t len = (size_t)(q2 - q1);
    if (len >= outSize) len = outSize - 1;
    memcpy(out, q1, len);
    out[len] = '\0';
    return 1;
}

static int parse_line(const char* line, char* deviceId, size_t deviceIdSize,
                      char* isoTs, size_t isoTsSize, float* temp, float* humi) {
    // Python이 보내는 JSON 키와 동일해야 함
    // {"deviceId":"jetson01","ts":"...","temperatureC":..,"humidityPct":..}
    int ok = 1;
    ok &= extract_string_field(line, "\"deviceId\"", deviceId, deviceIdSize);
    ok &= extract_string_field(line, "\"ts\"", isoTs, isoTsSize);
    ok &= extract_float_field(line, "\"temperatureC\"", temp);
    ok &= extract_float_field(line, "\"humidityPct\"", humi);
    return ok;
}

static int file_exists(const char* path) {
    return access(path, F_OK) == 0;
}

int main(void) {
    // CSV 파일 오픈(append)
    int newFile = !file_exists(OUT_CSV);

    FILE* out = fopen(OUT_CSV, "a");
    if (!out) {
        perror("fopen th_buffer.csv");
        return 1;
    }

    // 새 파일이면 헤더 한 번 작성
    if (newFile) {
        fprintf(out, "epoch,iso_ts,deviceId,temperatureC,humidityPct\n");
        fflush(out);
    }

    printf("[C] Opening FIFO for reading: %s\n", FIFO_PATH);
    printf("[C] Waiting for Python writer...\n");

    FILE* fifo = open_fifo_blocking();
    if (!fifo) {
        perror("fopen FIFO");
        fclose(out);
        return 1;
    }

    char line[4096];

    while (1) {
        if (!fgets(line, sizeof(line), fifo)) {
            if (feof(fifo)) {
                fprintf(stderr, "[C] FIFO EOF (writer closed). Re-opening FIFO...\n");
                fclose(fifo);
                fifo = open_fifo_blocking();
                if (!fifo) {
                    perror("[C] reopen FIFO failed");
                    sleep(1);
                }
                continue;
            }
            if (ferror(fifo)) {
                fprintf(stderr, "[C] FIFO read error: %s\n", strerror(errno));
                clearerr(fifo);
                sleep(1);
                continue;
            }
            sleep(1);
            continue;
        }

        char deviceId[64] = {0};
        char isoTs[64] = {0};
        float temp = 0.0f, humi = 0.0f;

        if (!parse_line(line, deviceId, sizeof(deviceId), isoTs, sizeof(isoTs), &temp, &humi)) {
            fprintf(stderr, "[C] parse failed: %s", line);
            continue;
        }

        long epoch = time(NULL); // 수신 시각(UTC epoch). "센서 측정 시각"이 필요하면 isoTs 파싱해서 epoch로 바꿀 수도 있음.

        // CSV 한 줄 append
        fprintf(out, "%ld,%s,%s,%.2f,%.2f\n", epoch, isoTs, deviceId, temp, humi);

        // 1~2초 주기면 flush로 안전성 확보(전원 OFF 대비). 너무 잦으면 성능↓지만 이 주기면 OK.
        fflush(out);

        printf("[C] buffered(csv): epoch=%ld iso=%s dev=%s temp=%.2f hum=%.2f\n",
               epoch, isoTs, deviceId, temp, humi);
    }

    fclose(fifo);
    fclose(out);
    return 0;
}
