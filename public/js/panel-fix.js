
(function() {
    // 1. Убираем inline style="display:none" со всех .panel элементов
    //    (inline стиль перекрывает CSS-класс .panel.active { display: block })
    document.querySelectorAll('.panel').forEach(function(p) {
      if (p.style.display === 'none') {
        p.style.display = '';
      }
    });
  
    // 2. Патчим app.showPanel — добавляем сброс inline-стиля
    var _originalShowPanel = app.showPanel.bind(app);
    app.showPanel = function(name) {
      // Сбросить inline display:none на всех панелях
      document.querySelectorAll('.panel').forEach(function(p) {
        if (p.style.display === 'none') p.style.display = '';
      });
      // Вызвать оригинальный showPanel
      _originalShowPanel(name);
      // Доп. гарантия — у активной панели убрать inline style
      var panel = document.getElementById('panel-' + name);
      if (panel) {
        panel.style.display = '';
        panel.classList.add('active');
      }
    };
  
    console.log('[panel-fix] ✅ Panel display fix applied');
  })();