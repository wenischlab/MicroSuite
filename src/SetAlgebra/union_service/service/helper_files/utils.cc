#include "utils.h"
#include <cstring>
#include <sstream>

std::string ExecuteShellCommand(const char* cmd)
{
    char buffer[128];
    std::string result = "";
    std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
    if (!pipe) throw std::runtime_error("popen() failed!");
    while (!feof(pipe.get())) {
        if (fgets(buffer, 128, pipe.get()) != NULL)
            result += buffer;
    }
    return result;
}

void StringSplit(std::string str, 
        char delimiter,
        std::vector<std::string>* queries_str_vec)
{
    std::stringstream ss(str); // Turn the string into a stream.
    std::string tok;

    while(std::getline(ss, tok, delimiter)) {
        if (queries_str_vec->size() == 2048) break;
        queries_str_vec->push_back(tok);
    }
}

void GetPerf(pid_t pid) {
    std::ostringstream str_pid;
    str_pid << pid;
    const std::string my_pid(str_pid.str());
    std::string s = "perf stat -e context-switches -I 60000 -p " + my_pid + " &";
    char* cmd = new char[s.length() + 1];
    std::strcpy(cmd, s.c_str());
    ExecuteShellCommand(cmd);
}
