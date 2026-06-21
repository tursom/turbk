import { defineComponent, h } from 'vue';
import { t } from '../i18n';

export default defineComponent({
  props: {
    title: { type: String, required: true },
    empty: { type: Boolean, required: true }
  },
  setup(props, { slots }) {
    return () =>
      h('section', { class: 'panel' }, [
        h('div', { class: 'panel-title' }, [h('h2', props.title), slots.actions?.()]),
        props.empty
          ? h('div', { class: 'empty-state' }, [h('span', t('common.noRecords'))])
          : h('div', { class: 'table-wrap' }, [h('table', slots.default?.())])
      ]);
  }
});
