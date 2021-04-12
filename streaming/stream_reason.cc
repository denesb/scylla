/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "streaming/stream_reason.hh"
#include <ostream>

namespace streaming {

std::ostream& operator<<(std::ostream& out, stream_reason r) {
    switch (r) {
	case stream_reason::unspecified: out << "unspecified"; break;
	case stream_reason::bootstrap: out << "bootstrap"; break;
	case stream_reason::decommission: out << "decommission"; break;
	case stream_reason::removenode: out << "removenode"; break;
	case stream_reason::rebuild: out << "rebuild"; break;
	case stream_reason::repair: out << "repair"; break;
	case stream_reason::replace: out << "replace"; break;
    }
    return out;
}

}
