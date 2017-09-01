$( document ).ready(function() {
  // Add buttons to allow menu expansion. Note: top-level items are treated differently from sub-menu items.
  $('ul.subnav').prev().prepend( "<a class='expander'>+</a>" );
  $('ul.subnav').parent().prevUntil('.toctree-l1').prev().prepend( "<a class='expander top-level'>+</a>" );

  // Collapse sub-menus.
  $('a.expander').parent().next().addClass( 'collapse' );
  $('a.expander.top-level').parent().next().next().addClass( 'collapse' );
  
  // Toggle the collapsed state on button clicks.
  $('a.expander').on('click', function () {
    if ( $(this).hasClass('top-level') ) {
      $(this).parent().next().next().toggleClass( 'collapse' );
    } else {
      $(this).parent().next().toggleClass( 'collapse' );
    }
    // Update the button to match new sub-menu state.
    $(this).trigger('expander');
  });

  // Change the button to match the (un-)collapsed state of the sub-menu.
  $('a.expander').on('expander', function () {
      $(this).html("-");
    if ( $(this).hasClass('top-level') ) {
      if ( $(this).parent().next().next().hasClass('collapse') ) {
        $(this).html("+");
      }
    } else {
      if ( $(this).parent().next().hasClass('collapse') ) {
        $(this).html("+");
      }
    }
  });

  // Expand the current menu tree.
  $('a.current').parents().removeClass( 'collapse' );

  // Update the button to match newly opened sub-menu state.
  $('a.current').each( function () {
    $('a.expander').trigger('expander');
  });

});


