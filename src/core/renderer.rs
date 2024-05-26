use std::collections::HashMap;
use crate::core::{Page, Tid, Tree};
use std::path::{Path};
use std::env;
use std::fs::{File};
use std::io::Write;
use handlebars::*;
use handlebars::Handlebars;
use serde_json::json;
use serde_json::Value::Object;
use serde_json::value::Value;

struct IsArrayHelper;

struct ContextHelper;

struct TidRenderHelper;

struct HasChildrenHelper;

struct SampleLookupHelper;

impl HelperDef for SampleLookupHelper {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let map = h.param(0).unwrap();
        let key = h.param(1).unwrap();
        let mp: HashMap<String, Vec<Page>> = serde_json::from_value(map.value().clone()).unwrap();
        let page: Page = serde_json::from_value(key.value().clone()).unwrap();
        let k = format!("{}", page.id);
        if page.is_leaf {
            return Ok(json!([]).into());
        }
        let val = match mp.get(&k) {
            Some(val) => val,
            None => {
                return Ok(json!([]).into());
            }
        };
        let val_json = json!(val);
        Ok(val_json.clone().into())
    }
}

impl HelperDef for HasChildrenHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();
        if param.is_value_missing() {
            // TODO: recheck why is this happening?
            out.write("false").unwrap();
            return Ok(());
        }
        let pages: Vec<Page> = serde_json::from_value(param.value().clone()).unwrap();
        if pages.len() == 0 {
            out.write("false").unwrap();
            return Ok(());
        }
        if pages[0].is_leaf {
            out.write("false").unwrap();
        } else {
            out.write("true").unwrap();
        }
        Ok(())
    }
}

impl HelperDef for IsArrayHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();

        // Check if the parameter is an array
        let is_array = match param.value() {
            JsonValue::Array(_) => true,
            _ => false,
        };

        if is_array {
            out.write("true").unwrap()
        } else {
            out.write("false").unwrap()
        }

        Ok(())
    }
}

impl HelperDef for ContextHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();
        // println!("CONTEXT param - {:?}", param);

        Ok(())
    }
}

impl HelperDef for TidRenderHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();
        let tid: Tid = serde_json::from_value(param.value().clone()).unwrap();
        let val = format!("({}, {})", tid.block_number, tid.offset_number);
        out.write(val.as_str()).unwrap();
        Ok(())
    }
}

// Implement your IsString helper
struct IsStringHelper;

impl HelperDef for IsStringHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();

        // Check if the parameter is a string
        let is_string = match param.value() {
            JsonValue::String(_) => true,
            _ => false,
        };

        if is_string {
            out.write("true").unwrap();
        } else {
            out.write("false").unwrap();
        }

        Ok(())
    }
}

fn get_parent_child_mapping(pages: Vec<Page>) -> HashMap<String, Vec<Page>> {
    let mut parent_child_map: HashMap<String, Vec<Page>> = HashMap::new();
    for page in pages {
        let mut child_pages: Vec<Page> = vec![];
        if page.is_leaf {
            // if leaf page, don't add.
            return parent_child_map;
        }
        for item in page.items.clone() {
            if let Some(child) = item.child {
                let mut child_hash_map = get_parent_child_mapping(vec![*child.clone()]);
                for (page, child) in child_hash_map {
                    if parent_child_map.contains_key(&page) {
                        let mut existing_child = parent_child_map.get_mut(&page).unwrap();
                        existing_child.append(&mut child.clone());
                    } else {
                        parent_child_map.insert(page, child.clone());
                    }
                }
                child_pages.push(*child);
            }
        }
        parent_child_map.insert(format!("{}", page.id), child_pages);
    }
    parent_child_map
}

pub fn render(tree: Tree, output_path: &Path) {
    let mut templates_dir = env::current_dir().unwrap();
    templates_dir = templates_dir.join("src").join("templates");
    let mut handlebars = Handlebars::new();


    let render_tree_file = templates_dir.join("render_tree.hbs");
    let render_page_file = templates_dir.join("render_page.hbs");
    let render_level_file = templates_dir.join("render_level.hbs");
    handlebars.register_template_file("render_tree", render_tree_file).unwrap();
    handlebars.register_template_file("render_page", render_page_file).unwrap();
    handlebars.register_template_file("render_level", render_level_file).unwrap();
    handlebars.register_helper("isArray", Box::new(IsArrayHelper));
    handlebars.register_helper("isString", Box::new(IsStringHelper));
    handlebars.register_helper("renderTid", Box::new(TidRenderHelper));
    handlebars.register_helper("hasChildren", Box::new(HasChildrenHelper));
    handlebars.register_helper("sample-lookup", Box::new(SampleLookupHelper));

    let mut map = serde_json::Map::new();
    map.insert("tree".to_string(), serde_json::to_value(&tree).unwrap());
    map.insert("index_type".to_string(), serde_json::to_value(&tree.index_type.unwrap()).unwrap());
    map.insert("parent_child_map".to_string(), serde_json::to_value(&get_parent_child_mapping(vec![tree.root.clone()])).unwrap());
    let rendered = handlebars.render("render_tree", &map).unwrap();
    let mut file = File::create(output_path).expect("Unable to create file");
    file.write_all(rendered.as_bytes()).expect("Unable to write data to file");
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use crate::core::{Page, Tid};
    use crate::core::structs::Item;

    #[test]
    pub fn test_string_to_page_conversion() {
        let original_page = Page {
            id: 0,
            level: 0,
            is_leaf: false,
            is_root: false,
            items: vec![Item {
                value: "abc".to_string(),
                child: None,
                pointer: None,
                obj_id: Some(Tid {
                    block_number: 1,
                    offset_number: 2,
                }),
            }],
            prev_page_id: Some(1),
            next_page_id: Some(1),
            high_key: None,
            prev_item: None,
            nb_items: None,
        };
        let v = vec![original_page];
        let json_str = json!(v).to_string();
        println!("{:?}", json_str);
        let page: Vec<Page> = serde_json::from_str(json_str.as_str()).unwrap();
        println!("{:?}", page);
    }

    #[test]
    pub fn test_get_parent_child_mapping() {
        let leaf_a = Page {
            id: 0,
            level: 0,
            is_leaf: true,
            is_root: false,
            items: vec![],
            prev_page_id: None,
            next_page_id: None,
            high_key: None,
            prev_item: None,
            nb_items: None,
        };
        let leaf_b = Page {
            id: 1,
            level: 0,
            is_leaf: true,
            is_root: false,
            items: vec![],
            prev_page_id: None,
            next_page_id: None,
            high_key: None,
            prev_item: None,
            nb_items: None,
        };
        let leaf_c = Page {
            id: 2,
            level: 0,
            is_leaf: true,
            is_root: false,
            items: vec![],
            prev_page_id: None,
            next_page_id: None,
            high_key: None,
            prev_item: None,
            nb_items: None,
        };
        let par_ab = Page {
            id: 3,
            level: 1,
            is_leaf: false,
            is_root: false,
            items: vec![Item {
                value: "abc".to_string(),
                child: Some(Box::new(leaf_a.clone())),
                pointer: None,
                obj_id: None,
            }, Item {
                value: "def".to_string(),
                child: Some(Box::new(leaf_b.clone())),
                pointer: None,
                obj_id: None,
            }],
            prev_page_id: None,
            next_page_id: None,
            high_key: None,
            prev_item: None,
            nb_items: None,
        };
        let par_c = Page {
            id: 4,
            level: 1,
            is_leaf: false,
            is_root: false,
            items: vec![Item {
                value: "ghi".to_string(),
                child: Some(Box::new(leaf_c.clone())),
                pointer: None,
                obj_id: None,
            }],
            prev_page_id: None,
            next_page_id: None,
            high_key: None,
            prev_item: None,
            nb_items: None,
        };
        let root = Page {
            id: 5,
            level: 2,
            is_leaf: false,
            is_root: true,
            items: vec![Item {
                value: "jkl".to_string(),
                child: Some(Box::new(par_ab.clone())),
                pointer: None,
                obj_id: None,
            }, Item {
                value: "mno".to_string(),
                child: Some(Box::new(par_c.clone())),
                pointer: None,
                obj_id: None,
            }],
            prev_page_id: None,
            next_page_id: None,
            high_key: None,
            prev_item: None,
            nb_items: None,
        };
        let mut expected_map = std::collections::HashMap::new();
        expected_map.insert("5".to_string(), vec![par_ab, par_c]);
        expected_map.insert("3".to_string(), vec![leaf_a, leaf_b]);
        expected_map.insert("4".to_string(), vec![leaf_c]);
        let parent_child_map = super::get_parent_child_mapping(vec![root]);
        assert_eq!(parent_child_map, expected_map);
    }
}