/*
 * Copyright (c) 2013-2018, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2018, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.cli

import groovy.transform.CompileStatic
import picocli.CommandLine
/**
 * CLI sub-command HELP
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
@CommandLine.Command(name = "help", description ="Print the usage help for a command")
class CmdHelp extends CmdBase {

    static final public NAME = 'help'

    @Override
    final String getName() { NAME }

    @CommandLine.Parameters(arity = "0..1", description = "Command name")
    List<String> args

    @Override
    void run() {
        String name = args ? args[0] : null
        def cmd = launcher.findCommand(name)
        if( cmd ) {
            launcher.usage(cmd)
        }
        else {
            println "Unknown command: $name"
        }
    }
}
