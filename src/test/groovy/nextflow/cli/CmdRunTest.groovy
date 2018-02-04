package nextflow.cli

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdRunTest extends Specification {

    def 'should create named parameters from args list' () {

        given:
        Map result
        def cmd = new CmdRun()

        when:
        result = cmd.makeParams( ['--foo', 'hello', '--bar', 'world'] )
        then:
        result.size()==2
        result.foo == 'hello'
        result.bar == 'world'

        when:
        result = cmd.makeParams( ['--foo', 'hello', 'world', '--bar', 'ciao'] )
        then:
        result.size()==2
        result.foo == ['hello', 'world']
        result.bar == 'ciao'

        when:
        result = cmd.makeParams( ['--foo', 'hello', '--foo', 'world', 'ciao'] )
        then:
        result.size()==1
        result.foo == ['hello', 'world', 'ciao']

        when:
        cmd.makeParams(['foo','bar'])
        then:
        thrown(IllegalArgumentException)

    }

}
