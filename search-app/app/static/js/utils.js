(function () {
  // const DB_NAMES = [];
  const TB_NAMES = [
    'raw_sample', 'ad_feature', 'user_profile', 'behavior_log'
  ];
  const COL_NAMES = {
    ad_feature: [
      'adgroup_id', 'cate_id', 'campaign_id',
      'brand', 'customer_id', 'price'
    ],
    user_profile: [
      'userid', 'cms_segid', 'cms_group_id',
      'final_gender_code', 'age_level',
      'pvalue_level', 'shopping_level', 'occupation',
      'new_user_class_level'
    ],
    behavior_log: [
      'nick', 'time_stamp', 'btag'
    ]
  };
  const AG_NAMES = [
    'max', 'min', 'avg',
    'count', 'count_unique', 'show_columns'
  ];

  function replaceSelection(event) {
    var oldSelection = document.querySelector('#col-select');
    var columnParent = oldSelection.closest('div.form-group');
    var tableSelection = document.querySelector('#ds-select');
    var curTableName = tableSelection.value;
    var options = COL_NAMES[curTableName];
    var newSelection = document.createElement('select');
    newSelection.setAttribute('id', oldSelection.id)
    newSelection.setAttribute('class', oldSelection.className)
    newSelection.setAttribute('name', oldSelection.name)
    var newOptions = options.forEach(option => {
      var newOption = document.createElement('option');
      newOption.setAttribute('value', option);
      newOption.textContent = option;
      newSelection.appendChild(newOption);
    });
    columnParent.replaceChild(newSelection, oldSelection);
  }

  var datasetBox = document.querySelector('#ds-select');
  datasetBox.addEventListener('change', replaceSelection);
})()