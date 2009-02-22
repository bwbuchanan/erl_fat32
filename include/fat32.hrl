-record(dirent, {short_name, name, attrib, cluster, created, accessed, modified, size}).

-define(ATTR_READONLY, 16#01).
-define(ATTR_HIDDEN,   16#02).
-define(ATTR_SYSTEM,   16#04).
-define(ATTR_VOLLBL,   16#08).
-define(ATTR_SUBDIR,   16#10).
-define(ATTR_ARCHIVE,  16#20).
-define(ATTR_UNUSED0,  16#40).
-define(ATTR_UNUSED1,  16#80).

-define(ATTR_LONGNAME, 16#0f).

%% Characters prohibited in Windows filenames:
%% \ / : * ? " < > |
%% And also restricted, maybe: [ ] ; = .  (need better reference, or testing)
