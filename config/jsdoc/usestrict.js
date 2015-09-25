'use strict';

// http://stackoverflow.com/questions/18861160/jsdoc-3-parse-error-getter-setter-with-the-same-name-in-strict-mode
exports.handlers = {
    beforeParse: function(e) {
        e.source = e.source.replace(/['"]use strict['"]/g, '//\'use strict\'');
    }
};
