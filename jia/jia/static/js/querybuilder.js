var qb = angular.module('jia.querybuilder', []);

qb.directive('querybuilder', function ($http, $compile) {
  var controller = ['$scope', function($scope) {
    $scope.nextStep = null;

    $scope.$watch('nextStep', function (newVal, oldVal) {
      if (newVal) {
        $scope.query.push($scope.nextStep);
        $scope.nextStep = null;
      }
    });

    $scope.delete = function (step) {
      var index = $scope.query.indexOf(step);
      if (index > -1) {
        $scope.query.splice(index, 1);
      }
    };
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/querybuilder.html',
    controller: controller,
    scope: {
      query: '='
    }
  };
});

qb.directive('operator', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.$watch('operator', function (newVal, oldVal) {
      if (!scope.newop && typeof newVal != 'undefined') {
        $http.get(['static', 'partials', 'operators', scope.operator.operator + '.html'].join('/'))
          .success(function(data, status, headers, config) {
            $(element).find('div.args').html(data);
            $compile(element.contents())(scope);
          });
      }
    });
  }

  var controller = ['$scope', function($scope) {
    $scope.operators = [
      {name: 'Transform', operator: 'transform', args: []},
      {name: 'Filter', operator: 'filter', args: []},
      {name: 'Order by', operator: 'orderby', args: []},
      {name: 'Limit', operator: 'limit', args: []},
      {name: 'Aggregate', operator: 'aggregate', args: []},
    ];
    $scope.args = [];
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operator.html',
    controller: controller,
    link: linker,
    scope: {
      operator: '=',
      newop: '='
    }
  };
});

qb.directive('cpf', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.argIndex = $(element).index();
  };

  var controller = ['$scope', function ($scope) {
    $scope.functions = [
      {
        name: 'Ceiling',
        value: 'ceiling',
        args: [],
        options: [
          {name: 'Property', type: 'property'},
          {name: 'Base', type: 'constant'},
          {name: 'Offset', type: 'constant'}
        ]
      },
      {
        name: 'Floor',
        value: 'floor',
        args: [],
        options: [
          {name: 'Property', type: 'property'},
          {name: 'Base', type: 'constant'},
          {name: 'Offset', type: 'constant'}
        ]
      },
      {
        name: 'Date Truncate',
        value: 'datetrunc',
        args: [],
        options: [
          {name: 'Property', type: 'property'},
          {name: 'Time scale', type: 'constant'}
        ]
      },
      {
        name: 'Date Part',
        value: 'datepart',
        args: [],
        options: [
          {name: 'Property', type: 'property'},
          {name: 'Time scale', type: 'constant'}
        ]
      },
      {
        name: 'Lowercase',
        value: 'lowercase',
        args: [],
        options: [
          {name: 'Property', type: 'property'}
        ]
      },
      {
        name: 'Uppercase',
        value: 'uppercase',
        args: [],
        options: [
          {name: 'Property', type: 'property'}
        ]
      },
      {
        name: 'Random Integer',
        value: 'randint',
        args: [],
        options: [
          {name: 'Low', type: 'constant'},
          {name: 'High', type: 'constant'}
        ]
      }
      /* 'Add': ['Property 1', 'Property 2'], */
      /* 'Subtract': ['Property 1', 'Property 2'], */
      /* 'Length': ['?'], */
    ];
    $scope.func = $scope.functions[0];

    $scope.types = [
      {name: 'Property', type: 'property'},
      {name: 'Constant', type: 'constant'},
      {name: 'Function', type: 'function'},
    ];
    $scope.type = $scope.types[0];

    $scope.args = [];
        
    $scope.$watch(function () {
      return [$scope.func,
              $scope.type,
              $scope.name,
              $scope.value,
              $scope.args];
    }, function () {
      var args = [];
      _.each($scope.args, function (arg, index) {
        var type = $scope.func.options[index].type;
        var cpf = {
          'cpf_type': type
        };
        if (type == 'property') {
          cpf['property_name'] = arg;
        }
        else if (type == 'constant') {
          cpf['constant_value'] = arg;
        }
        args.push(cpf);
      });
      $scope.$parent.operator.args[$scope.argIndex] = {
        'cpf_type': $scope.type.type,
        'function_name': $scope.func.value,
        'function_args': args,
        'property_name': $scope.name,
        'constant_value': $scope.value
      };
    }, true);

  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/cpf.html',
    controller: controller,
    link: linker,
    scope: {}
  };
});

qb.directive('op', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.argIndex = $(element).index();
  };

  var controller = ['$scope', function ($scope) {
    $scope.types = [
      {name: 'is less than', value: 'lt'},
      {name: 'is less than or equal to', value: 'lte'},
      {name: 'is greater than', value: 'gt'},
      {name: 'is greater than or equal to', value: 'gte'},
      {name: 'is equal to', value: 'eq'},
      {name: 'contains', value: 'contains'},
      {name: 'is in', value: 'in'},
      {name: 'matches regex', value: 'regex'}
    ];
    $scope.type = $scope.types[0];

    $scope.$watch('type', function () {
      $scope.$parent.operator.args[$scope.argIndex] = $scope.type.value;
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/op.html',
    controller: controller,
    link: linker,
    scope: {}
  };
});

qb.directive('aggtype', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.argIndex = $(element).index();
  };

  var controller = ['$scope', function ($scope) {
    $scope.types = [
      {name: 'Minimum', value: 'min'},
      {name: 'Maximum', value: 'max'},
      {name: 'Average', value: 'avg'},
      {name: 'Count', value: 'count'},
      {name: 'Sum', value: 'sum'},
      {name: 'Value count', value: 'valuecount'}
    ];
    $scope.type = $scope.types[3];

    $scope.$watch('type', function () {
      $scope.$parent.operator.args[$scope.argIndex] = $scope.type.value;
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/op.html',
    controller: controller,
    link: linker,
    scope: {}
  };
});

qb.directive('val', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.argIndex = $(element).index();
  };

  var controller = ['$scope', function ($scope) {
    $scope.$watch('val', function () {
      $scope.$parent.operator.args[$scope.argIndex] = $scope.val;
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/input.html',
    controller: controller,
    link: linker,
    scope: {}
  };
});

qb.directive('prop', function ($http, $compile) {
  var linker = function (scope, element, attrs) {
    scope.argIndex = $(element).index();
  };

  var controller = ['$scope', function ($scope) {
    $scope.$watch('val', function () {
      $scope.$parent.operator.args[$scope.argIndex] = {
        'cpf_type': 'property',
        'property_name': $scope.val
      }
    });
  }];

  return {
    restrict: "E",
    templateUrl: '/static/partials/operators/input.html',
    controller: controller,
    link: linker,
    scope: {}
  };
});
