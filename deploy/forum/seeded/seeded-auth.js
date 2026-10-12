app.initializers.add('seeded-auth', function () {
  // Flarum 1.8 exposes core modules on flarum.core.compat without the forum/ or common/ prefix.
  var compat = flarum.core.compat;
  var extend = compat['extend'].extend;
  var LogInButtons = compat['components/LogInButtons'];
  var LogInButton = compat['components/LogInButton'];
  extend(LogInButtons.prototype, 'items', function (items) {
    items.add('seeded', m(LogInButton, { className: 'Button LogInButton--seeded', icon: 'fas fa-seedling', path: '/auth/seeded' }, 'Sign in with seeded.art'));
  });
});

// Flarum boots every frontend file as an extension module and reads its extenders.
module.exports = { extend: [] };
