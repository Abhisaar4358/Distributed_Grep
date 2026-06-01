

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <random>
#include <filesystem>
#include <iomanip>
#include <sstream>

using namespace std;
namespace fs = filesystem;

static const vector<string> WORDS = {
    "the","quick","brown","fox","jumped","over","lazy","dog",
    "a","an","is","was","are","were","be","been","being",
    "have","has","had","do","does","did","will","would","shall",
    "should","may","might","must","can","could","need","dare",
    "ought","used","able","about","above","across","after","against",
    "along","among","around","at","before","behind","below","beneath",
    "beside","between","beyond","by","despite","during","except","for",
    "from","in","inside","into","near","of","off","on","onto","out",
    "outside","over","past","regarding","since","through","throughout",
    "to","toward","under","underneath","until","up","upon","with",
    "within","without","data","cluster","machine","network","compute",
    "processing","distributed","system","file","disk","memory","cpu",
    "task","worker","master","parallel","batch","job","queue","node",
    "server","client","index","search","pattern","match","record",
    "output","input","key","value","partition","reduce","map","sort",
    "hash","stream","buffer","cache","log","error","fault","replica",
    "backup","latency","throughput","bandwidth","shuffle","pipeline",
    "algorithm","function","library","framework","implementation","design",
    "computer","science","research","paper","result","experiment","test",
    "performance","benchmark","measurement","analysis","evaluation","model",
    "large","small","high","low","fast","slow","efficient","scalable",
    "reliable","available","consistent","replicated","distributed","sharded",
};

static const vector<string> SENTENCE_STARTS = {
    "The", "A", "Our", "This", "Each", "Every", "Some", "Many",
    "All", "Several", "No", "One", "Two", "Three",
};


static string random_sentence(mt19937& rng, const string& inject = "")
{
    uniform_int_distribution<size_t> start_dist(0, SENTENCE_STARTS.size()-1);
    uniform_int_distribution<size_t> word_dist(0, WORDS.size()-1);
    uniform_int_distribution<int>    len_dist(6, 14);

    ostringstream oss;
    oss << SENTENCE_STARTS[start_dist(rng)];

    int n = len_dist(rng);
    for (int i = 0; i < n; ++i) {
        oss << " " << WORDS[word_dist(rng)];
    }

    if (!inject.empty()) {
        uniform_int_distribution<int> pos_dist(1, n);
        ostringstream injected;
        istringstream reader(oss.str());
        string word;
        int pos = 0;
        while (reader >> word) {
            if (pos == pos_dist(rng)) injected << " " << inject;
            injected << " " << word;
            ++pos;
        }
        return injected.str() + ".";
    }

    oss << ".";
    return oss.str();
}

int main(int argc, char* argv[])
{
    if (argc < 2) {
        cerr << "Usage: " << argv[0]
                  << " <output_dir> [num_files] [lines_per_file] [rare_pattern]\n";
        return 1;
    }

    string output_dir    = argv[1];
    int         num_files     = (argc >= 3) ? stoi(argv[2]) : 10;
    int         lines_per_file= (argc >= 4) ? stoi(argv[3]) : 5000;
    string rare_pattern  = (argc >= 5) ? argv[4] : "XQZ";

    fs::create_directories(output_dir);

    random_device rd;
    mt19937 rng(rd());

    long total_lines   = static_cast<long>(num_files) * lines_per_file;
    long rare_count    = max(1L, total_lines / 500L);  

    uniform_int_distribution<long> line_dist(0, total_lines - 1);
    vector<bool> is_rare(total_lines, false);
    long injected = 0;
    while (injected < rare_count) {
        long idx = line_dist(rng);
        if (!is_rare[idx]) { is_rare[idx] = true; ++injected; }
    }

    cout << "Generating " << num_files << " files, "
              << lines_per_file << " lines each.\n";
    cout << "Rare pattern '" << rare_pattern << "' will appear in "
              << rare_count << " / " << total_lines << " lines.\n";

    long global_line = 0;
    for (int f = 0; f < num_files; ++f) {

        ostringstream fname;
        fname << output_dir << "/input-"
              << setw(3) << setfill('0') << f << ".txt";

        ofstream out(fname.str());
        if (!out) {
            cerr << "Cannot write " << fname.str() << "\n";
            return 1;
        }

        for (int l = 0; l < lines_per_file; ++l, ++global_line) {
            bool inject = is_rare[global_line];
            out << random_sentence(rng, inject ? rare_pattern : "") << "\n";
        }

        cout << "  Written: " << fname.str() << "\n";
    }

    cout << "\nData generation complete.\n";
    cout << "To run the grep job, search for pattern: " << rare_pattern << "\n";
    return 0;
}
