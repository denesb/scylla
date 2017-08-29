SRC_DIR=$1
GEN_DIR=$2

mkdir -p ${GEN_DIR}/cql3

sed -e '/^#if 0/,/^#endif/d' ${SRC_DIR}/cql3/Cql.g > ${GEN_DIR}/cql3/Cql.g \
     && antlr3 ${GEN_DIR}/cql3/Cql.g \
     && sed -i -e 's/^\( *\)\(ImplTraits::CommonTokenType\* [a-zA-Z0-9_]* = NULL;\)/\1const \2/' \
        -e '1i using ExceptionBaseType = int;' \
        -e 's/^{/{ ExceptionBaseType\* ex = nullptr;/; s/ExceptionBaseType\* ex = new/ex = new/' \
            ${GEN_DIR}/cql3/CqlParser.cpp
