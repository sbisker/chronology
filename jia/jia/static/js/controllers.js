function Controller($scope) {
// Get the users saved templates
// Store them in $scope.views
// Last element of $scope.views should be 
// <ng-include src="newVis"></ng-include>

    $scope.views = [];

    $scope.views.push({ text: "<ng-include src='newVis'></ng-include>" });
}
