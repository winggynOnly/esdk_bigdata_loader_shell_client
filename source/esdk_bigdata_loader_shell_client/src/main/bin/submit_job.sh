#!/bin/bash
main()
{
    scriptPath=$(dirname "$0")
    scriptPath=$(cd "$scriptPath"; pwd)
    shellClientHome=$scriptPath

    if [ $# -eq 0 ] ; then
        cat "$shellClientHome/conf/options.ini"
        echo ""
        return 2
    fi

    classpath="${shellClientHome}/lib/*"

    java -version > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        echo "The jre is not installed."
        return 2
    fi

    local krb5File="${shellClientHome}/conf/kdc.conf"
    if [ ! -f "${krb5File}" ] ; then
        if [ -z ${KRB5_FILE} ] ; then
            echo "The krb5 file ${krb5File} does not exist."
            return 2
        else
            krb5File=${KRB5_FILE}
        fi
    fi

    java -cp "${classpath}" -Dshell.client.home="${shellClientHome}"  -Djava.security.krb5.conf="${krb5File}" \
         -Dsun.security.krb5.debug=false org.apache.loader.shell.client.main.SubmitJob "$@"
    local retVal=$?
    if [ $retVal -ne 0 ] ; then
        retVal=1
    fi

    return $retVal
}

main "$@"
exit $?
