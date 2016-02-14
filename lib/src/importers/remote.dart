part of dataset;

class Remote implements Importer {
  /**
   * Responsible for fetching data from a url.
   *
   * @constructor
   * @name Remote
   * @memberof Miso.Dataset.Importers
   * @augments Miso.Dataset.Importers
   *
   * @param {Object} options
   * @param {String} options.url - url to query
   * @param {Function} options.extract - a method to pass raw data through
   *                                     before handing back to parser.
   * @param {String} options.dataType - ajax datatype
   * @param {Boolean} options.jsonp - true if it's a jsonp request, false
   *                                  otherwise.
   *
   * @externalExample {runnable} importers/remote
   */
  Remote(options) {
    options = options || {};

    this._url = options.url;
    this.extract = options.extract || this.extract;

    // Default ajax request parameters
    this.params = {
      type: "GET",
      url: _.isFunction(this._url) ? _.bind(this._url, this) : this._url,
      dataType: options.dataType
          ? options.dataType
          : (options.jsonp ? "jsonp" : "json"),
      callback: options.callback,
      headers: options.headers
    };
  }

  fetch(options) {
    // call the original fetch method of object parsing.
    // we are assuming the parsed version of the data will
    // be an array of objects.
    var callback = _.bind((data) {
      options.success(this.extract(data));
    }, this);

    // do we have a named callback? We need to wrap our
    // success callback in this name
    if (this.callback) {
      window[this.callback] = callback;
    }

    // make ajax call to fetch remote url.
    Dataset.Xhr(_.extend(this.params, {
      success: this.callback ? this.callback : callback,
      error: options.error
    }));
  }
}

// this XHR code is from @rwldron.
var _xhrSetup = {
  url: "",
  data: "",
  dataType: "",
  success: () {},
  type: "GET",
  async: true,
  headers: {},
  xhr: () {
    return global.ActiveXObject
        ? new global.ActiveXObject("Microsoft.XMLHTTP")
        : new global.XMLHttpRequest();
  }
},
    rparams = r"\?";

class Xhr {
  Xhr(options) {
    // json|jsonp etc.
    options.dataType =
        options.dataType && options.dataType.toLowerCase() || null;

    var url = _.isFunction(options.url) ? options.url() : options.url;

    if (options.dataType &&
        (options.dataType == "jsonp" || options.dataType == "script")) {
      Dataset.Xhr.getJSONP(url, options.success, options.dataType == "script",
          options.error, options.callback);

      return;
    }

    var settings = _.extend({}, _xhrSetup, options, {url: url});

    // create new xhr object
    settings.ajax = settings.xhr();

    if (settings.ajax) {
      if (settings.type == "GET" && settings.data) {
        //  append query string
        settings.url +=
            (rparams.test(settings.url) ? "&" : "?") + settings.data;

        //  Garbage collect and reset settings.data
        settings.data = null;
      }

      settings.ajax.open(settings.type, settings.url, settings.async);

      // Add custom headers
      if (options.headers) {
        _(options.headers).forEach((value, name) {
          settings.ajax.setRequestHeader(name, value);
        });
      }

      settings.ajax.send(settings.data || null);

      return Xhr.httpData(settings);
    }
  }

  getJSONP(url, success, isScript, error, callback) {
    // If this is a script request, ensure that we do not
    // call something that has already been loaded
    if (isScript) {
      var scripts = document.querySelectorAll("script[src=\"" + url + "\"]");

      //  If there are scripts with this url loaded, early return
      if (scripts.length) {
        //  Execute success callback and pass "exists" flag
        if (success) {
          success(true);
        }

        return;
      }
    }

    var head = document.head ||
            document.getElementsByTagName("head")[0] ||
            document.documentElement,
        script = document.createElement("script"),
        paramStr = url.split("?")[1],
        isFired = false,
        params = [],
        parts;

    // Extract params
    if (paramStr && !isScript) {
      params = paramStr.split("&");
    }
    if (params.length) {
      parts = params[params.length - 1].split("=");
    }
    if (!callback) {
      var fallback = _.uniqueId("callback");
      callback = params.length ? (parts[1] ? parts[1] : fallback) : fallback;
    }

    if (!paramStr && !isScript) {
      url += "?";
    }

    if (!paramStr || !r"callback".test(paramStr)) {
      if (paramStr) {
        url += "&";
      }
      url += "callback=" + callback;
    }

    if (callback && !isScript) {
      // If a callback name already exists
      if (!!window[callback]) {
        callback = callback + (/*+*/ new Date()) + _.uniqueId();
      }

      //  Define the JSONP success callback globally
      window[callback] = (data) {
        if (success) {
          success(data);
        }
        isFired = true;
      };

      //  Replace callback param and callback name
      if (parts) {
        url = url.replace(parts.join("="), parts[0] + "=" + callback);
      }
    }

    script.onload = script.onreadystatechange = () {
      if (!script.readyState || r"loaded|complete".test(script.readyState)) {
        //  Handling remote script loading callbacks
        if (isScript) {
          //  getScript
          if (success) {
            success();
          }
        }

        //  Executing for JSONP requests
        if (isFired) {
          //  Garbage collect the callback
          try {
            /*delete*/ window[callback];
          } catch (e) {
            window[callback] = /*void*/ 0;
          }

          //  Garbage collect the script resource
          head.removeChild(script);
        }
      }
    };

    script.onerror = (e) {
      if (error) {
        error.call(null, e);
      }
    };

    script.src = url;
    head.insertBefore(script, head.firstChild);
    return;
  }

  httpData(settings) {
    var data, json = null;

    handleResponse() {
      if (settings.ajax.readyState == 4) {
        try {
          json = JSON.parse(settings.ajax.responseText);
        } catch (e) {
          // suppress
        }

        data = {
          xml: settings.ajax.responseXML,
          text: settings.ajax.responseText,
          json: json
        };

        if (settings.dataType) {
          data = data[settings.dataType];
        }

        // if we got an ok response, call success, otherwise fail.
        if (r"(2..)".test(settings.ajax.status)) {
          settings.success.call(settings.ajax, data);
        } else {
          if (settings.error) {
            settings.error.call(null, settings.ajax.statusText);
          }
        }
      }
    }

    if (settings.ajax.readyState == 4) {
      handleResponse(); // Internet Exploder doesn't bother with readystatechange handlers for cached resources... trigger the handler manually
    } else {
      settings.ajax.onreadystatechange = handleResponse;
    }

    return data;
  }
}
