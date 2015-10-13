#!/bin/bash
main()
{
    if [ $# -ne 1 ]; then
      echo "Parameter is empty. Please input password. for example: bash pwd.sh PASSWORD"
      return 1
    fi

    scriptPath=$(dirname "$0")
    scriptPath=$(cd "$scriptPath"; pwd)

    shellClientHome=$scriptPath

    classpath="${shellClientHome}/../lib/*"

    java -version > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        echo "The jre is not installed."
        return 2
    fi

    java -cp "${classpath}" -Dshell.client.home="${shellClientHome}" org.apache.loader.shell.client.PasswordEncryption $1
    local retVal=$?
    if [ $retVal -ne 0 ] ; then
        retVal=1
    fi

    return ${retVal}
}

main "$@"