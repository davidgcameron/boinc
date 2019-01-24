// This file is part of BOINC.
// http://boinc.berkeley.edu
// Copyright (C) 2008 University of California
//
// BOINC is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation,
// either version 3 of the License, or (at your option) any later version.
//
// BOINC is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with BOINC.  If not, see <http://www.gnu.org/licenses/>.

// A sample assimilator that:
// 1) if success, copy the output file(s) to a directory
// 2) if failure, append a message to an error log

#include <vector>
#include <string>
#include <cstdlib>

#include "boinc_db.h"
#include "error_numbers.h"
#include "filesys.h"
#include "sched_msgs.h"
#include "validate_util.h"
#include "sched_config.h"

using std::vector;
using std::string;

const char* outdir = "/tmp/grid";

int assimilate_handler_init(int argc, char** argv) {
    for (int i=1; i<argc; i++) {
        if (!strcmp(argv[i], "--outdir")) {
            outdir = argv[++i];
        } else {
            fprintf(stderr, "bad arg %s\n", argv[i]);
        }
    }
    return 0;
}

void assimilate_handler_usage() {
    // describe the project specific arguments here
    fprintf(stderr,
        "    Custom options:\n"
        "    [--outdir X]  output dir for result files\n"
    );
}

int assimilate_handler(
    WORKUNIT& wu, vector<RESULT>& /*results*/, RESULT& canonical_result
) {
    int retval;

    if (wu.canonical_resultid) {
        vector<OUTPUT_FILE_INFO> output_files;
        char dest_dir[200];
        char cmd[2000];
        get_output_file_infos(canonical_result, output_files);
        sprintf(dest_dir, "%s/%s", outdir, wu.name);
    
        OUTPUT_FILE_INFO& fi = output_files[0];
        if (output_files.size() == 2) { // HITS is separate
            OUTPUT_FILE_INFO& fi2 = output_files[1];
            sprintf(cmd, "if [ -d %s ]; then /bin/tar xvf %s -C %s && /bin/rm -f %s; mv %s/%s.diag %s/; mv %s %s/HITS.pool.root.1; fi", dest_dir, fi.path.c_str(), dest_dir, fi.path.c_str(), dest_dir, wu.name, outdir, fi2.path.c_str(), dest_dir);
        } else {
            sprintf(cmd, "if [ -d %s ]; then /bin/tar xvf %s -C %s && /bin/rm -f %s; mv %s/%s.diag %s/; fi", dest_dir, fi.path.c_str(), dest_dir, fi.path.c_str(), dest_dir, wu.name, outdir);
        }
        printf("result files: %lu, command: %s\n", output_files.size(), cmd);
        retval = system(cmd);
        if (retval) {
           printf("%s: no output file\n", wu.name);
        }
    } else {
        printf("%s: no result\n", wu.name);
    }
    return 0;
}
